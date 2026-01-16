#![allow(clippy::print_stderr, clippy::print_stdout)] // allowed in this module only

use core::num::NonZeroU32;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use ironrdp::graphics::pointer::DecodedPointer;
use raw_window_handle::{DisplayHandle, HasDisplayHandle as _};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};
use winit::application::ApplicationHandler;
use winit::dpi::{LogicalPosition, PhysicalSize};
use winit::event::{self, WindowEvent};
use winit::event_loop::{ActiveEventLoop, ControlFlow, EventLoop};
use winit::platform::scancode::PhysicalKeyExtScancode as _;
use winit::window::{CursorIcon, CustomCursor, Window, WindowAttributes};

use crate::rdp::{RdpInputEvent, RdpOutputEvent, SessionId};

type WindowSurface = (Arc<Window>, softbuffer::Surface<DisplayHandle<'static>, Arc<Window>>);

/// Per-session state for blended rendering
struct SessionState {
    buffer: Vec<u32>,
    width: u16,
    height: u16,
    pointer: Option<Arc<DecodedPointer>>,
    pointer_visible: bool,
}

impl SessionState {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            width: 0,
            height: 0,
            pointer: None,
            pointer_visible: true,
        }
    }
}

pub struct App {
    input_event_sender: mpsc::UnboundedSender<RdpInputEvent>,
    context: softbuffer::Context<DisplayHandle<'static>>,
    window: Option<WindowSurface>,
    /// Per-session frame buffers for alpha blending
    session_buffers: HashMap<SessionId, SessionState>,
    /// Composited buffer for display
    composited_buffer: Vec<u32>,
    /// Size of the composited buffer (max of all sessions)
    buffer_size: (u16, u16),
    input_database: ironrdp::input::Database,
    last_size: Option<PhysicalSize<u32>>,
    resize_timeout: Option<Instant>,
}

impl App {
    pub fn new(
        event_loop: &EventLoop<RdpOutputEvent>,
        input_event_sender: &mpsc::UnboundedSender<RdpInputEvent>,
    ) -> anyhow::Result<Self> {
        // SAFETY: We drop the softbuffer context right before the event loop is stopped, thus making this safe.
        // FIXME: This is not a sufficient proof and the API is actually unsound as-is.
        let display_handle = unsafe {
            core::mem::transmute::<DisplayHandle<'_>, DisplayHandle<'static>>(
                event_loop.display_handle().context("get display handle")?,
            )
        };
        let context = softbuffer::Context::new(display_handle)
            .map_err(|e| anyhow::anyhow!("unable to initialize softbuffer context: {e}"))?;

        let input_database = ironrdp::input::Database::new();
        Ok(Self {
            input_event_sender: input_event_sender.clone(),
            context,
            window: None,
            session_buffers: HashMap::new(),
            composited_buffer: Vec::new(),
            buffer_size: (0, 0),
            input_database,
            last_size: None,
            resize_timeout: None,
        })
    }

    fn send_resize_event(&mut self) {
        let Some(size) = self.last_size.take() else {
            return;
        };
        let Some((window, _)) = self.window.as_mut() else {
            return;
        };
        #[expect(clippy::as_conversions, reason = "casting f64 to u32")]
        let scale_factor = (window.scale_factor() * 100.0) as u32;

        let width = u16::try_from(size.width).expect("reasonable width");
        let height = u16::try_from(size.height).expect("reasonable height");

        let _ = self.input_event_sender.send(RdpInputEvent::Resize {
            width,
            height,
            scale_factor,
            // TODO: it should be possible to get the physical size here, however winit doesn't make it straightforward.
            // FreeRDP does it based on DPI reading grabbed via [`SDL_GetDisplayDPI`](https://wiki.libsdl.org/SDL2/SDL_GetDisplayDPI):
            // https://github.com/FreeRDP/FreeRDP/blob/ba8cf8cf2158018fb7abbedb51ab245f369be813/client/SDL/sdl_monitor.cpp#L250-L262
            // See also: https://github.com/rust-windowing/winit/issues/826
            physical_size: None,
        });
    }

    /// Composite all session buffers using alpha blending (each session has alpha = 1/n)
    fn composite_buffers(&mut self) {
        let num_sessions = self.session_buffers.len();
        if num_sessions == 0 {
            self.composited_buffer.clear();
            return;
        }

        // Find the maximum dimensions across all sessions
        let (max_width, max_height) = self.session_buffers.values().fold((0u16, 0u16), |(w, h), state| {
            (w.max(state.width), h.max(state.height))
        });

        if max_width == 0 || max_height == 0 {
            return;
        }

        self.buffer_size = (max_width, max_height);
        let total_pixels = (max_width as usize) * (max_height as usize);

        // Initialize composited buffer with black (transparent)
        self.composited_buffer.clear();
        self.composited_buffer.resize(total_pixels, 0);

        // Alpha blend each session's buffer using additive blending
        // Each session contributes with weight = 1/n, summed together
        // This ensures equal contribution and full brightness when all sessions show the same image
        for state in self.session_buffers.values() {
            if state.buffer.is_empty() || state.width == 0 || state.height == 0 {
                continue;
            }

            for y in 0..(state.height as usize).min(max_height as usize) {
                for x in 0..(state.width as usize).min(max_width as usize) {
                    let src_idx = y * (state.width as usize) + x;
                    let dst_idx = y * (max_width as usize) + x;

                    if src_idx < state.buffer.len() && dst_idx < self.composited_buffer.len() {
                        let src_pixel = state.buffer[src_idx];
                        let dst_pixel = self.composited_buffer[dst_idx];

                        // Extract RGB components (format: 0x00RRGGBB in big-endian)
                        let src_r = (src_pixel >> 16) & 0xFF;
                        let src_g = (src_pixel >> 8) & 0xFF;
                        let src_b = src_pixel & 0xFF;

                        let dst_r = (dst_pixel >> 16) & 0xFF;
                        let dst_g = (dst_pixel >> 8) & 0xFF;
                        let dst_b = dst_pixel & 0xFF;

                        // Additive blending: add src/n to the accumulator
                        // Each session contributes 1/n of its color value
                        let r = (dst_r + src_r / num_sessions as u32).min(255);
                        let g = (dst_g + src_g / num_sessions as u32).min(255);
                        let b = (dst_b + src_b / num_sessions as u32).min(255);

                        self.composited_buffer[dst_idx] = (r << 16) | (g << 8) | b;
                    }
                }
            }
        }
    }

    /// Create a combined cursor from all session pointers
    fn update_combined_cursor(&mut self, event_loop: &ActiveEventLoop) {
        let Some((window, _)) = self.window.as_mut() else {
            return;
        };

        // Collect all visible pointers
        let visible_pointers: Vec<&Arc<DecodedPointer>> = self
            .session_buffers
            .values()
            .filter(|s| s.pointer_visible && s.pointer.is_some())
            .filter_map(|s| s.pointer.as_ref())
            .collect();

        if visible_pointers.is_empty() {
            window.set_cursor(CursorIcon::default());
            window.set_cursor_visible(true);
            return;
        }

        // If only one pointer, use it directly
        if visible_pointers.len() == 1 {
            let pointer = visible_pointers[0];
            match CustomCursor::from_rgba(
                pointer.bitmap_data.clone(),
                pointer.width,
                pointer.height,
                pointer.hotspot_x,
                pointer.hotspot_y,
            ) {
                Ok(cursor) => window.set_cursor(event_loop.create_custom_cursor(cursor)),
                Err(error) => error!(?error, "Failed to set cursor bitmap"),
            }
            window.set_cursor_visible(true);
            return;
        }

        // Combine multiple pointers by alpha blending them
        // Find max dimensions
        let max_width = visible_pointers.iter().map(|p| p.width).max().unwrap_or(0);
        let max_height = visible_pointers.iter().map(|p| p.height).max().unwrap_or(0);

        if max_width == 0 || max_height == 0 {
            window.set_cursor(CursorIcon::default());
            return;
        }

        let total_pixels = (max_width * max_height) as usize;
        let mut combined_rgba = vec![0u8; total_pixels * 4];

        let alpha_per_pointer = 255 / visible_pointers.len() as u32;

        for pointer in &visible_pointers {
            let pw = pointer.width as usize;
            let ph = pointer.height as usize;

            for y in 0..ph.min(max_height as usize) {
                for x in 0..pw.min(max_width as usize) {
                    let src_idx = (y * pw + x) * 4;
                    let dst_idx = (y * max_width as usize + x) * 4;

                    if src_idx + 3 < pointer.bitmap_data.len() && dst_idx + 3 < combined_rgba.len() {
                        let src_r = pointer.bitmap_data[src_idx] as u32;
                        let src_g = pointer.bitmap_data[src_idx + 1] as u32;
                        let src_b = pointer.bitmap_data[src_idx + 2] as u32;
                        let src_a = pointer.bitmap_data[src_idx + 3] as u32;

                        let dst_r = combined_rgba[dst_idx] as u32;
                        let dst_g = combined_rgba[dst_idx + 1] as u32;
                        let dst_b = combined_rgba[dst_idx + 2] as u32;
                        let dst_a = combined_rgba[dst_idx + 3] as u32;

                        // Blend with session alpha
                        let eff_alpha = (src_a * alpha_per_pointer) / 255;
                        let inv_alpha = 255 - eff_alpha;

                        combined_rgba[dst_idx] = ((dst_r * inv_alpha + src_r * eff_alpha) / 255) as u8;
                        combined_rgba[dst_idx + 1] = ((dst_g * inv_alpha + src_g * eff_alpha) / 255) as u8;
                        combined_rgba[dst_idx + 2] = ((dst_b * inv_alpha + src_b * eff_alpha) / 255) as u8;
                        combined_rgba[dst_idx + 3] = (dst_a + eff_alpha).min(255) as u8;
                    }
                }
            }
        }

        // Use the first pointer's hotspot as the combined hotspot
        let hotspot_x = visible_pointers[0].hotspot_x;
        let hotspot_y = visible_pointers[0].hotspot_y;

        match CustomCursor::from_rgba(combined_rgba, max_width as u16, max_height as u16, hotspot_x, hotspot_y) {
            Ok(cursor) => window.set_cursor(event_loop.create_custom_cursor(cursor)),
            Err(error) => {
                error!(?error, "Failed to create combined cursor");
                window.set_cursor(CursorIcon::default());
            }
        }
        window.set_cursor_visible(true);
    }

    fn draw(&mut self) {
        if self.composited_buffer.is_empty() {
            return;
        }
        let Some((_, surface)) = self.window.as_mut() else {
            return;
        };
        let mut sb_buffer = surface.buffer_mut().expect("surface buffer");
        sb_buffer.copy_from_slice(self.composited_buffer.as_slice());
        sb_buffer.present().expect("buffer present");
    }

    fn remove_session(&mut self, session_id: SessionId) {
        self.session_buffers.remove(&session_id);
        info!(?session_id, remaining = self.session_buffers.len(), "Session removed");
        
        // Recomposite with remaining sessions
        if !self.session_buffers.is_empty() {
            self.composite_buffers();
        }
    }

    fn has_active_sessions(&self) -> bool {
        !self.session_buffers.is_empty()
    }
}

impl ApplicationHandler<RdpOutputEvent> for App {
    fn about_to_wait(&mut self, event_loop: &ActiveEventLoop) {
        if let Some(timeout) = self.resize_timeout {
            if let Some(timeout) = timeout.checked_duration_since(Instant::now()) {
                event_loop.set_control_flow(ControlFlow::wait_duration(timeout));
            } else {
                self.send_resize_event();
                self.resize_timeout = None;
                event_loop.set_control_flow(ControlFlow::Wait);
            }
        }
    }

    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        let window_attributes = WindowAttributes::default().with_title("IronRDP");
        match event_loop.create_window(window_attributes) {
            Ok(window) => {
                let window = Arc::new(window);
                let surface = softbuffer::Surface::new(&self.context, Arc::clone(&window)).expect("surface");
                self.window = Some((window, surface));
            }
            Err(error) => {
                error!(%error, "Failed to create window");
                event_loop.exit();
            }
        }
    }

    fn window_event(&mut self, event_loop: &ActiveEventLoop, window_id: winit::window::WindowId, event: WindowEvent) {
        let Some((window, _)) = self.window.as_mut() else {
            return;
        };
        if window_id != window.id() {
            return;
        }

        match event {
            WindowEvent::Resized(size) => {
                self.last_size = Some(size);
                self.resize_timeout = Some(Instant::now() + Duration::from_secs(1));
            }
            WindowEvent::CloseRequested => {
                if self.input_event_sender.send(RdpInputEvent::Close).is_err() {
                    error!("Failed to send graceful shutdown event, closing the window");
                    event_loop.exit();
                }
            }
            WindowEvent::DroppedFile(_) => {
                // TODO(#110): File upload
            }
            // WindowEvent::ReceivedCharacter(_) => {
            // Sadly, we can't use this winit event to send RDP unicode events because
            // of the several reasons:
            // 1. `ReceivedCharacter` event doesn't provide a way to distinguish between
            //    key press and key release, therefore the only way to use it is to send
            //    a key press + release events sequentially, which will not allow to
            //    handle long press and key repeat events.
            // 2. This event do not fire for non-printable keys (e.g. Control, Alt, etc.)
            // 3. This event fies BEFORE `KeyboardInput` event, so we can't make a
            //    reasonable workaround for `1` and `2` by collecting physical key press
            //    information first via `KeyboardInput` before processing `ReceivedCharacter`.
            //
            // However, all of these issues can be solved by updating `winit` to the
            // newer version.
            //
            // TODO(#376): Update winit
            // TODO(#376): Implement unicode input in native client
            // }
            WindowEvent::KeyboardInput { event, .. } => {
                if let Some(scancode) = event.physical_key.to_scancode() {
                    let scancode = match u16::try_from(scancode) {
                        Ok(scancode) => scancode,
                        Err(_) => {
                            warn!("Unsupported scancode: `{scancode:#X}`; ignored");
                            return;
                        }
                    };
                    let scancode = ironrdp::input::Scancode::from_u16(scancode);

                    let operation = match event.state {
                        event::ElementState::Pressed => ironrdp::input::Operation::KeyPressed(scancode),
                        event::ElementState::Released => ironrdp::input::Operation::KeyReleased(scancode),
                    };

                    let input_events = self.input_database.apply(core::iter::once(operation));

                    send_fast_path_events(&self.input_event_sender, input_events);
                }
            }
            WindowEvent::ModifiersChanged(modifiers) => {
                const SHIFT_LEFT: ironrdp::input::Scancode = ironrdp::input::Scancode::from_u8(false, 0x2A);
                const CONTROL_LEFT: ironrdp::input::Scancode = ironrdp::input::Scancode::from_u8(false, 0x1D);
                const ALT_LEFT: ironrdp::input::Scancode = ironrdp::input::Scancode::from_u8(false, 0x38);
                const LOGO_LEFT: ironrdp::input::Scancode = ironrdp::input::Scancode::from_u8(true, 0x5B);

                let mut operations = smallvec::SmallVec::<[ironrdp::input::Operation; 4]>::new();

                let mut add_operation = |pressed: bool, scancode: ironrdp::input::Scancode| {
                    let operation = if pressed {
                        ironrdp::input::Operation::KeyPressed(scancode)
                    } else {
                        ironrdp::input::Operation::KeyReleased(scancode)
                    };
                    operations.push(operation);
                };

                // NOTE: https://docs.rs/winit/0.30.12/src/winit/keyboard.rs.html#1737-1744
                //
                // We can’t use state.lshift_state(), state.lcontrol_state(), etc, because on some platforms such as
                // Linux, the modifiers change is hidden.
                //
                // > The exact modifier key is not used to represent modifiers state in the
                // > first place due to a fact that modifiers state could be changed without any
                // > key being pressed and on some platforms like Wayland/X11 which key resulted
                // > in modifiers change is hidden, also, not that it really matters.
                add_operation(modifiers.state().shift_key(), SHIFT_LEFT);
                add_operation(modifiers.state().control_key(), CONTROL_LEFT);
                add_operation(modifiers.state().alt_key(), ALT_LEFT);
                add_operation(modifiers.state().super_key(), LOGO_LEFT);

                let input_events = self.input_database.apply(operations);

                send_fast_path_events(&self.input_event_sender, input_events);
            }
            WindowEvent::CursorMoved { position, .. } => {
                let win_size = window.inner_size();
                #[expect(clippy::as_conversions, reason = "casting f64 to u16")]
                let x = (position.x / f64::from(win_size.width) * f64::from(self.buffer_size.0)) as u16;
                #[expect(clippy::as_conversions, reason = "casting f64 to u16")]
                let y = (position.y / f64::from(win_size.height) * f64::from(self.buffer_size.1)) as u16;
                let operation = ironrdp::input::Operation::MouseMove(ironrdp::input::MousePosition { x, y });

                let input_events = self.input_database.apply(core::iter::once(operation));

                send_fast_path_events(&self.input_event_sender, input_events);
            }
            WindowEvent::MouseWheel { delta, .. } => {
                let mut operations = smallvec::SmallVec::<[ironrdp::input::Operation; 2]>::new();

                match delta {
                    event::MouseScrollDelta::LineDelta(delta_x, delta_y) => {
                        if delta_x.abs() > 0.001 {
                            operations.push(ironrdp::input::Operation::WheelRotations(
                                ironrdp::input::WheelRotations {
                                    is_vertical: false,
                                    #[expect(clippy::as_conversions, reason = "casting f32 to i16")]
                                    rotation_units: (delta_x * 100.) as i16,
                                },
                            ));
                        }

                        if delta_y.abs() > 0.001 {
                            operations.push(ironrdp::input::Operation::WheelRotations(
                                ironrdp::input::WheelRotations {
                                    is_vertical: true,
                                    #[expect(clippy::as_conversions, reason = "casting f32 to i16")]
                                    rotation_units: (delta_y * 100.) as i16,
                                },
                            ));
                        }
                    }
                    event::MouseScrollDelta::PixelDelta(delta) => {
                        if delta.x.abs() > 0.001 {
                            operations.push(ironrdp::input::Operation::WheelRotations(
                                ironrdp::input::WheelRotations {
                                    is_vertical: false,
                                    #[expect(clippy::as_conversions, reason = "casting f64 to i16")]
                                    rotation_units: delta.x as i16,
                                },
                            ));
                        }

                        if delta.y.abs() > 0.001 {
                            operations.push(ironrdp::input::Operation::WheelRotations(
                                ironrdp::input::WheelRotations {
                                    is_vertical: true,
                                    #[expect(clippy::as_conversions, reason = "casting f64 to i16")]
                                    rotation_units: delta.y as i16,
                                },
                            ));
                        }
                    }
                };

                let input_events = self.input_database.apply(operations);

                send_fast_path_events(&self.input_event_sender, input_events);
            }
            WindowEvent::MouseInput { state, button, .. } => {
                let mouse_button = match button {
                    event::MouseButton::Left => ironrdp::input::MouseButton::Left,
                    event::MouseButton::Right => ironrdp::input::MouseButton::Right,
                    event::MouseButton::Middle => ironrdp::input::MouseButton::Middle,
                    event::MouseButton::Back => ironrdp::input::MouseButton::X1,
                    event::MouseButton::Forward => ironrdp::input::MouseButton::X2,
                    event::MouseButton::Other(native_button) => {
                        if let Some(button) = ironrdp::input::MouseButton::from_native_button(native_button) {
                            button
                        } else {
                            return;
                        }
                    }
                };

                let operation = match state {
                    event::ElementState::Pressed => ironrdp::input::Operation::MouseButtonPressed(mouse_button),
                    event::ElementState::Released => ironrdp::input::Operation::MouseButtonReleased(mouse_button),
                };

                let input_events = self.input_database.apply(core::iter::once(operation));

                send_fast_path_events(&self.input_event_sender, input_events);
            }
            WindowEvent::RedrawRequested => {
                self.draw();
            }
            WindowEvent::ActivationTokenDone { .. }
            | WindowEvent::Moved(_)
            | WindowEvent::Destroyed
            | WindowEvent::HoveredFile(_)
            | WindowEvent::HoveredFileCancelled
            | WindowEvent::Focused(_)
            | WindowEvent::Ime(_)
            | WindowEvent::CursorEntered { .. }
            | WindowEvent::CursorLeft { .. }
            | WindowEvent::PinchGesture { .. }
            | WindowEvent::PanGesture { .. }
            | WindowEvent::DoubleTapGesture { .. }
            | WindowEvent::RotationGesture { .. }
            | WindowEvent::TouchpadPressure { .. }
            | WindowEvent::AxisMotion { .. }
            | WindowEvent::Touch(_)
            | WindowEvent::ScaleFactorChanged { .. }
            | WindowEvent::ThemeChanged(_)
            | WindowEvent::Occluded(_) => {
                // ignore
            }
        }
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, event: RdpOutputEvent) {
        match event {
            RdpOutputEvent::Image { session_id, buffer, width, height } => {
                trace!(width = ?width, height = ?height, ?session_id, "Received image with size");
                
                // Update this session's buffer
                let state = self.session_buffers.entry(session_id).or_insert_with(SessionState::new);
                state.buffer = buffer;
                state.width = width.get();
                state.height = height.get();

                // Recomposite all buffers
                self.composite_buffers();

                // Now access window for resize and redraw
                if let Some((window, surface)) = self.window.as_mut() {
                    trace!(window_physical_size = ?window.inner_size(), "Drawing composited image to the window");
                    
                    if self.buffer_size.0 > 0 && self.buffer_size.1 > 0 {
                        let w = NonZeroU32::new(self.buffer_size.0 as u32).unwrap();
                        let h = NonZeroU32::new(self.buffer_size.1 as u32).unwrap();
                        surface.resize(w, h).expect("surface resize");
                    }

                    window.request_redraw();
                }
            }
            RdpOutputEvent::ConnectionFailure { session_id, error } => {
                error!(?error, ?session_id, "Connection failure");
                eprintln!("Connection error for session {:?}: {}", session_id, error.report());
                
                // Remove this session
                self.remove_session(session_id);
                
                // Only exit if no sessions remain
                if !self.has_active_sessions() {
                    event_loop.exit();
                }
            }
            RdpOutputEvent::SessionTerminated { session_id, result } => {
                match &result {
                    Ok(reason) => {
                        info!(?session_id, ?reason, "Session terminated gracefully");
                    }
                    Err(error) => {
                        error!(?session_id, ?error, "Session terminated with error");
                        eprintln!("Session {:?} error: {}", session_id, error.report());
                    }
                }
                
                // Remove this session
                self.remove_session(session_id);
                
                // Update cursor after session removal
                self.update_combined_cursor(event_loop);
                
                // Only exit if no sessions remain
                if !self.has_active_sessions() {
                    println!("All sessions ended");
                    event_loop.exit();
                } else {
                    // Recomposite and redraw with remaining sessions
                    self.composite_buffers();
                    if let Some((window, _)) = self.window.as_ref() {
                        window.request_redraw();
                    }
                }
            }
            RdpOutputEvent::AllSessionsEnded => {
                info!("All sessions have ended");
                event_loop.exit();
            }
            RdpOutputEvent::PointerHidden { session_id } => {
                if let Some(state) = self.session_buffers.get_mut(&session_id) {
                    state.pointer_visible = false;
                }
                self.update_combined_cursor(event_loop);
            }
            RdpOutputEvent::PointerDefault { session_id } => {
                if let Some(state) = self.session_buffers.get_mut(&session_id) {
                    state.pointer = None;
                    state.pointer_visible = true;
                }
                self.update_combined_cursor(event_loop);
            }
            RdpOutputEvent::PointerPosition { session_id: _, x, y } => {
                // Position is shared across all sessions, just set it once
                if let Some((window, _)) = self.window.as_ref() {
                    if let Err(error) = window.set_cursor_position(LogicalPosition::new(x, y)) {
                        error!(?error, "Failed to set cursor position");
                    }
                }
            }
            RdpOutputEvent::PointerBitmap { session_id, pointer } => {
                debug!(?session_id, width = ?pointer.width, height = ?pointer.height, "Received pointer bitmap");
                
                // Store pointer for this session
                let state = self.session_buffers.entry(session_id).or_insert_with(SessionState::new);
                state.pointer = Some(pointer);
                state.pointer_visible = true;
                
                // Update combined cursor
                self.update_combined_cursor(event_loop);
            }
        }
    }
}

fn send_fast_path_events(
    input_event_sender: &mpsc::UnboundedSender<RdpInputEvent>,
    input_events: smallvec::SmallVec<[ironrdp::pdu::input::fast_path::FastPathInputEvent; 2]>,
) {
    if !input_events.is_empty() {
        let _ = input_event_sender.send(RdpInputEvent::FastPath(input_events));
    }
}
