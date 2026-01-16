use core::num::NonZeroU16;
use std::sync::Arc;

use ironrdp::cliprdr::backend::{ClipboardMessage, CliprdrBackendFactory};
use ironrdp::connector::connection_activation::ConnectionActivationState;
use ironrdp::connector::{ConnectionResult, ConnectorResult};
use ironrdp::displaycontrol::client::DisplayControlClient;
use ironrdp::displaycontrol::pdu::MonitorLayoutEntry;
use ironrdp::graphics::image_processing::PixelFormat;
use ironrdp::graphics::pointer::DecodedPointer;
use ironrdp::pdu::input::fast_path::FastPathInputEvent;
use ironrdp::pdu::{pdu_other_err, PduResult};
use ironrdp::session::image::DecodedImage;
use ironrdp::session::{fast_path, ActiveStage, ActiveStageOutput, GracefulDisconnectReason, SessionResult};
use ironrdp::svc::SvcMessage;
use ironrdp::{cliprdr, connector, rdpdr, rdpsnd, session};
use ironrdp_core::WriteBuf;
use ironrdp_dvc_pipe_proxy::DvcNamedPipeProxy;
use ironrdp_rdpsnd_native::cpal;
use ironrdp_tokio::reqwest::ReqwestNetworkClient;
use ironrdp_tokio::{single_sequence_step_read, split_tokio_framed, FramedWrite};
use rdpdr::NoopRdpdrBackend;
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, trace, warn};
use winit::event_loop::EventLoopProxy;

use crate::config::{Config, Destination, RDCleanPathConfig};

/// Unique identifier for each RDP session
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub usize);

#[derive(Debug)]
pub enum RdpOutputEvent {
    Image {
        session_id: SessionId,
        buffer: Vec<u32>,
        width: NonZeroU16,
        height: NonZeroU16,
    },
    ConnectionFailure {
        session_id: SessionId,
        error: connector::ConnectorError,
    },
    PointerDefault {
        session_id: SessionId,
    },
    PointerHidden {
        session_id: SessionId,
    },
    PointerPosition {
        session_id: SessionId,
        x: u16,
        y: u16,
    },
    PointerBitmap {
        session_id: SessionId,
        pointer: Arc<DecodedPointer>,
    },
    SessionTerminated {
        session_id: SessionId,
        result: SessionResult<GracefulDisconnectReason>,
    },
    /// All sessions have ended
    AllSessionsEnded,
}

#[derive(Debug)]
pub enum RdpInputEvent {
    Resize {
        width: u16,
        height: u16,
        scale_factor: u32,
        /// The physical size of the display in millimeters (width, height).
        physical_size: Option<(u32, u32)>,
    },
    /// FastPath events are broadcast to all sessions
    FastPath(SmallVec<[FastPathInputEvent; 2]>),
    Close,
    /// Clipboard events are broadcast to all sessions
    Clipboard(ClipboardMessage),
    SendDvcMessages {
        channel_id: u32,
        messages: Vec<SvcMessage>,
    },
}

impl RdpInputEvent {
    pub fn create_channel() -> (mpsc::UnboundedSender<Self>, mpsc::UnboundedReceiver<Self>) {
        mpsc::unbounded_channel()
    }
}

/// Internal event sent from the coordinator to individual session tasks
/// These are broadcast events that need to be sent to multiple sessions
#[derive(Debug)]
pub enum SessionInputEvent {
    Resize {
        width: u16,
        height: u16,
        scale_factor: u32,
        physical_size: Option<(u32, u32)>,
    },
    FastPath(SmallVec<[FastPathInputEvent; 2]>),
    Close,
}

/// Clonable version of resize event for broadcasting
#[derive(Debug, Clone)]
struct ResizeEvent {
    width: u16,
    height: u16,
    scale_factor: u32,
    physical_size: Option<(u32, u32)>,
}

/// Broadcast channel message type (only for events that can be cloned)
#[derive(Debug, Clone)]
enum BroadcastEvent {
    Resize(ResizeEvent),
    FastPath(SmallVec<[FastPathInputEvent; 2]>),
    Close,
    /// Clipboard message wrapped in Arc since ClipboardMessage doesn't implement Clone
    Clipboard(Arc<ClipboardMessage>),
    /// DVC messages wrapped in Arc<Mutex<Option>> - only first session to receive will process
    /// (SvcMessage doesn't implement Clone, so we use take-once semantics)
    DvcMessages { channel_id: u32, messages: Arc<std::sync::Mutex<Option<Vec<SvcMessage>>>> },
}

/// Internal event sent from individual session tasks to the coordinator
#[derive(Debug)]
pub enum SessionOutputEvent {
    Image {
        buffer: Vec<u32>,
        width: NonZeroU16,
        height: NonZeroU16,
    },
    ConnectionFailure(connector::ConnectorError),
    PointerDefault,
    PointerHidden,
    PointerPosition { x: u16, y: u16 },
    PointerBitmap(Arc<DecodedPointer>),
    Terminated(SessionResult<GracefulDisconnectReason>),
    /// Request reconnection with new size
    ReconnectWithNewSize { width: u16, height: u16 },
}

pub struct DvcPipeProxyFactory {
    rdp_input_sender: mpsc::UnboundedSender<RdpInputEvent>,
}

impl Clone for DvcPipeProxyFactory {
    fn clone(&self) -> Self {
        Self {
            rdp_input_sender: self.rdp_input_sender.clone(),
        }
    }
}

impl DvcPipeProxyFactory {
    pub fn new(rdp_input_sender: mpsc::UnboundedSender<RdpInputEvent>) -> Self {
        Self { rdp_input_sender }
    }

    pub fn create(&self, channel_name: String, pipe_name: String) -> DvcNamedPipeProxy {
        let rdp_input_sender = self.rdp_input_sender.clone();

        DvcNamedPipeProxy::new(&channel_name, &pipe_name, move |channel_id, messages| {
            rdp_input_sender
                .send(RdpInputEvent::SendDvcMessages { channel_id, messages })
                .map_err(|_error| pdu_other_err!("send DVC messages to the event loop",))?;

            Ok(())
        })
    }
}

pub type WriteDvcMessageFn = Box<dyn Fn(u32, SvcMessage) -> PduResult<()> + Send + 'static>;

/// State for a single session running in its own task
struct SessionTask {
    session_id: SessionId,
    destination: Destination,
    config: Config,
    event_loop_proxy: EventLoopProxy<RdpOutputEvent>,
    broadcast_receiver: broadcast::Receiver<BroadcastEvent>,
    cliprdr_factory: Option<Arc<std::sync::Mutex<Box<dyn CliprdrBackendFactory + Send>>>>,
}

impl SessionTask {
    async fn run(self) {
        let SessionTask {
            session_id,
            destination,
            mut config,
            event_loop_proxy,
            mut broadcast_receiver,
            cliprdr_factory,
        } = self;
        
        loop {
            let result = run_single_session(
                session_id,
                config.clone(),
                destination.clone(),
                event_loop_proxy.clone(),
                broadcast_receiver,
                cliprdr_factory.clone(),
            ).await;
            
            match result {
                Ok((SessionControlFlow::ReconnectWithNewSize { width, height }, rx)) => {
                    config.connector.desktop_size.width = width;
                    config.connector.desktop_size.height = height;
                    broadcast_receiver = rx;
                    info!(session_id = ?session_id, width, height, "Reconnecting with new size");
                }
                Ok((SessionControlFlow::TerminatedGracefully(reason), _rx)) => {
                    let _ = event_loop_proxy.send_event(RdpOutputEvent::SessionTerminated {
                        session_id,
                        result: Ok(reason),
                    });
                    break;
                }
                Err((e, _rx)) => {
                    let _ = event_loop_proxy.send_event(RdpOutputEvent::SessionTerminated {
                        session_id,
                        result: Err(e),
                    });
                    break;
                }
            }
        }
    }
}

/// Run a single session attempt, returning the broadcast receiver so it can be reused for reconnects
async fn run_single_session(
    session_id: SessionId,
    config: Config,
    destination: Destination,
    event_loop_proxy: EventLoopProxy<RdpOutputEvent>,
    broadcast_receiver: broadcast::Receiver<BroadcastEvent>,
    cliprdr_factory: Option<Arc<std::sync::Mutex<Box<dyn CliprdrBackendFactory + Send>>>>,
) -> Result<(SessionControlFlow, broadcast::Receiver<BroadcastEvent>), (session::SessionError, broadcast::Receiver<BroadcastEvent>)> {
    // Create DVC proxy factory for this session (uses a dummy sender since DVC is not fully supported in multi-session)
    let (dummy_sender, _) = mpsc::unbounded_channel();
    let dvc_pipe_proxy_factory = DvcPipeProxyFactory::new(dummy_sender);

    let connect_result = if let Some(rdcleanpath) = config.rdcleanpath.clone() {
        connect_ws_for_destination(
            config,
            destination,
            rdcleanpath,
            cliprdr_factory,
            dvc_pipe_proxy_factory,
        )
        .await
    } else {
        connect_for_destination(
            config,
            destination,
            cliprdr_factory,
            dvc_pipe_proxy_factory,
        )
        .await
    };

    let (connection_result, framed) = match connect_result {
        Ok(result) => result,
        Err(e) => {
            let _ = event_loop_proxy.send_event(RdpOutputEvent::ConnectionFailure {
                session_id,
                error: e,
            });
            return Err((session::general_err!("Connection failed"), broadcast_receiver));
        }
    };

    let result = active_session_for_task(
        session_id,
        framed,
        connection_result,
        event_loop_proxy,
        broadcast_receiver,
    )
    .await;
    
    match result {
        (Ok(flow), rx) => Ok((flow, rx)),
        (Err(e), rx) => Err((e, rx)),
    }
}

enum SessionControlFlow {
    ReconnectWithNewSize { width: u16, height: u16 },
    TerminatedGracefully(GracefulDisconnectReason),
}

/// Multi-session RDP client that manages multiple concurrent sessions
pub struct MultiSessionClient {
    pub config: Config,
    pub event_loop_proxy: EventLoopProxy<RdpOutputEvent>,
    pub input_event_receiver: mpsc::UnboundedReceiver<RdpInputEvent>,
    pub cliprdr_factory: Option<Arc<std::sync::Mutex<Box<dyn CliprdrBackendFactory + Send>>>>,
}

impl MultiSessionClient {
    pub async fn run(mut self) {
        use tokio::task::LocalSet;
        
        let destinations = self.config.destinations.clone();
        let num_sessions = destinations.len();
        
        if num_sessions == 0 {
            error!("No destinations configured");
            let _ = self.event_loop_proxy.send_event(RdpOutputEvent::AllSessionsEnded);
            return;
        }

        info!(num_sessions, "Starting multi-session RDP client");

        // Create broadcast channel for events that need to go to all sessions
        let (broadcast_tx, _) = broadcast::channel::<BroadcastEvent>(256);
        
        let mut active_sessions: std::collections::HashSet<SessionId> = std::collections::HashSet::new();

        // Use LocalSet to spawn tasks that don't require Send
        let local = LocalSet::new();

        // Spawn a task for each destination
        for (idx, destination) in destinations.into_iter().enumerate() {
            let session_id = SessionId(idx);
            let broadcast_rx = broadcast_tx.subscribe();
            active_sessions.insert(session_id);

            let task = SessionTask {
                session_id,
                destination,
                config: self.config.clone(),
                event_loop_proxy: self.event_loop_proxy.clone(),
                broadcast_receiver: broadcast_rx,
                cliprdr_factory: self.cliprdr_factory.clone(),
            };

            local.spawn_local(task.run());
        }

        // Run input forwarding concurrently with the local tasks
        let event_loop_proxy = self.event_loop_proxy.clone();
        local.run_until(async move {
            // Main event loop: broadcast input events to all sessions
            loop {
                let input_event = match self.input_event_receiver.recv().await {
                    Some(event) => event,
                    None => {
                        debug!("Input channel closed, shutting down");
                        break;
                    }
                };

                // Convert RdpInputEvent to BroadcastEvent and send
                let broadcast_event = match input_event {
                    RdpInputEvent::Resize { width, height, scale_factor, physical_size } => {
                        Some(BroadcastEvent::Resize(ResizeEvent { width, height, scale_factor, physical_size }))
                    }
                    RdpInputEvent::FastPath(events) => Some(BroadcastEvent::FastPath(events)),
                    RdpInputEvent::Close => Some(BroadcastEvent::Close),
                    // Clipboard messages are broadcast to all sessions
                    RdpInputEvent::Clipboard(msg) => Some(BroadcastEvent::Clipboard(Arc::new(msg))),
                    // DVC messages - only first session to receive will process (SvcMessage isn't Clone)
                    RdpInputEvent::SendDvcMessages { channel_id, messages } => {
                        warn!("Broadcasting DVC messages to all sessions; only the first to receive will process them.");
                        Some(BroadcastEvent::DvcMessages { 
                            channel_id, 
                            messages: Arc::new(std::sync::Mutex::new(Some(messages))) 
                        })
                    }
                };

                if let Some(event) = broadcast_event {
                    // If send fails, it means no receivers are left
                    if broadcast_tx.send(event).is_err() {
                        info!("All session receivers dropped, ending");
                        break;
                    }
                }
            }

            let _ = event_loop_proxy.send_event(RdpOutputEvent::AllSessionsEnded);
        }).await;
    }
}

// Keep the old RdpClient for backwards compatibility (single session)
pub struct RdpClient {
    pub config: Config,
    pub event_loop_proxy: EventLoopProxy<RdpOutputEvent>,
    pub input_event_receiver: mpsc::UnboundedReceiver<RdpInputEvent>,
    pub cliprdr_factory: Option<Box<dyn CliprdrBackendFactory + Send>>,
    pub dvc_pipe_proxy_factory: DvcPipeProxyFactory,
}

impl RdpClient {
    pub async fn run(mut self) {
        let session_id = SessionId(0);
        loop {
            let (connection_result, framed) = if let Some(rdcleanpath) = self.config.rdcleanpath.as_ref() {
                match connect_ws(
                    &self.config,
                    rdcleanpath,
                    self.cliprdr_factory.as_deref(),
                    &self.dvc_pipe_proxy_factory,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        let _ = self.event_loop_proxy.send_event(RdpOutputEvent::ConnectionFailure {
                            session_id,
                            error: e,
                        });
                        break;
                    }
                }
            } else {
                match connect(
                    &self.config,
                    self.cliprdr_factory.as_deref(),
                    &self.dvc_pipe_proxy_factory,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        let _ = self.event_loop_proxy.send_event(RdpOutputEvent::ConnectionFailure {
                            session_id,
                            error: e,
                        });
                        break;
                    }
                }
            };

            match active_session(
                session_id,
                framed,
                connection_result,
                &self.event_loop_proxy,
                &mut self.input_event_receiver,
            )
            .await
            {
                Ok(RdpControlFlow::ReconnectWithNewSize { width, height }) => {
                    self.config.connector.desktop_size.width = width;
                    self.config.connector.desktop_size.height = height;
                }
                Ok(RdpControlFlow::TerminatedGracefully(reason)) => {
                    let _ = self.event_loop_proxy.send_event(RdpOutputEvent::SessionTerminated {
                        session_id,
                        result: Ok(reason),
                    });
                    break;
                }
                Err(e) => {
                    let _ = self.event_loop_proxy.send_event(RdpOutputEvent::SessionTerminated {
                        session_id,
                        result: Err(e),
                    });
                    break;
                }
            }
        }
        let _ = self.event_loop_proxy.send_event(RdpOutputEvent::AllSessionsEnded);
    }
}

enum RdpControlFlow {
    ReconnectWithNewSize { width: u16, height: u16 },
    TerminatedGracefully(GracefulDisconnectReason),
}

trait AsyncReadWrite: AsyncRead + AsyncWrite {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite {}

type UpgradedFramed = ironrdp_tokio::TokioFramed<Box<dyn AsyncReadWrite + Unpin + Send + Sync>>;

async fn connect(
    config: &Config,
    cliprdr_factory: Option<&(dyn CliprdrBackendFactory + Send)>,
    dvc_pipe_proxy_factory: &DvcPipeProxyFactory,
) -> ConnectorResult<(ConnectionResult, UpgradedFramed)> {
    let dest = format!("{}:{}", config.destination().name(), config.destination().port());

    let stream = TcpStream::connect(dest)
        .await
        .map_err(|e| connector::custom_err!("TCP connect", e))?;
    let client_addr = stream
        .local_addr()
        .map_err(|e| connector::custom_err!("get socket local address", e))?;
    let mut framed = ironrdp_tokio::TokioFramed::new(stream);

    let mut drdynvc =
        ironrdp::dvc::DrdynvcClient::new().with_dynamic_channel(DisplayControlClient::new(|_| Ok(Vec::new())));

    // Instantiate all DVC proxies
    for proxy in config.dvc_pipe_proxies.iter() {
        let channel_name = proxy.channel_name.clone();
        let pipe_name = proxy.pipe_name.clone();

        trace!(%channel_name, %pipe_name, "Creating DVC proxy");

        drdynvc = drdynvc.with_dynamic_channel(dvc_pipe_proxy_factory.create(channel_name, pipe_name));
    }

    let mut connector = connector::ClientConnector::new(config.connector.clone(), client_addr)
        .with_static_channel(drdynvc)
        .with_static_channel(rdpsnd::client::Rdpsnd::new(Box::new(cpal::RdpsndBackend::new())))
        .with_static_channel(rdpdr::Rdpdr::new(Box::new(NoopRdpdrBackend {}), "IronRDP".to_owned()).with_smartcard(0));

    if let Some(builder) = cliprdr_factory {
        let backend = builder.build_cliprdr_backend();

        let cliprdr = cliprdr::Cliprdr::new(backend);

        connector.attach_static_channel(cliprdr);
    }

    let should_upgrade = ironrdp_tokio::connect_begin(&mut framed, &mut connector).await?;

    debug!("TLS upgrade");

    // Ensure there is no leftover
    let (initial_stream, leftover_bytes) = framed.into_inner();

    let (upgraded_stream, tls_cert) = ironrdp_tls::upgrade(initial_stream, config.destination().name())
        .await
        .map_err(|e| connector::custom_err!("TLS upgrade", e))?;

    let upgraded = ironrdp_tokio::mark_as_upgraded(should_upgrade, &mut connector);

    let erased_stream: Box<dyn AsyncReadWrite + Unpin + Send + Sync> = Box::new(upgraded_stream);
    let mut upgraded_framed = ironrdp_tokio::TokioFramed::new_with_leftover(erased_stream, leftover_bytes);

    let server_public_key = ironrdp_tls::extract_tls_server_public_key(&tls_cert)
        .ok_or_else(|| connector::general_err!("unable to extract tls server public key"))?;
    let connection_result = ironrdp_tokio::connect_finalize(
        upgraded,
        connector,
        &mut upgraded_framed,
        &mut ReqwestNetworkClient::new(),
        config.destination().into(),
        server_public_key.to_owned(),
        None,
    )
    .await?;

    debug!(?connection_result);

    Ok((connection_result, upgraded_framed))
}

async fn connect_ws(
    config: &Config,
    rdcleanpath: &RDCleanPathConfig,
    cliprdr_factory: Option<&(dyn CliprdrBackendFactory + Send)>,
    dvc_pipe_proxy_factory: &DvcPipeProxyFactory,
) -> ConnectorResult<(ConnectionResult, UpgradedFramed)> {
    let hostname = rdcleanpath
        .url
        .host_str()
        .ok_or_else(|| connector::general_err!("host missing from the URL"))?;

    let port = rdcleanpath.url.port_or_known_default().unwrap_or(443);

    let socket = TcpStream::connect((hostname, port))
        .await
        .map_err(|e| connector::custom_err!("TCP connect", e))?;

    socket
        .set_nodelay(true)
        .map_err(|e| connector::custom_err!("set TCP_NODELAY", e))?;

    let client_addr = socket
        .local_addr()
        .map_err(|e| connector::custom_err!("get socket local address", e))?;

    let (ws, _) = tokio_tungstenite::client_async_tls(rdcleanpath.url.as_str(), socket)
        .await
        .map_err(|e| connector::custom_err!("WS connect", e))?;

    let ws = crate::ws::websocket_compat(ws);

    let mut framed = ironrdp_tokio::TokioFramed::new(ws);

    let mut drdynvc =
        ironrdp::dvc::DrdynvcClient::new().with_dynamic_channel(DisplayControlClient::new(|_| Ok(Vec::new())));

    // Instantiate all DVC proxies
    for proxy in config.dvc_pipe_proxies.iter() {
        let channel_name = proxy.channel_name.clone();
        let pipe_name = proxy.pipe_name.clone();

        trace!(%channel_name, %pipe_name, "Creating DVC proxy");

        drdynvc = drdynvc.with_dynamic_channel(dvc_pipe_proxy_factory.create(channel_name, pipe_name));
    }

    let mut connector = connector::ClientConnector::new(config.connector.clone(), client_addr)
        .with_static_channel(drdynvc)
        .with_static_channel(rdpsnd::client::Rdpsnd::new(Box::new(cpal::RdpsndBackend::new())))
        .with_static_channel(rdpdr::Rdpdr::new(Box::new(NoopRdpdrBackend {}), "IronRDP".to_owned()).with_smartcard(0));

    if let Some(builder) = cliprdr_factory {
        let backend = builder.build_cliprdr_backend();

        let cliprdr = cliprdr::Cliprdr::new(backend);

        connector.attach_static_channel(cliprdr);
    }

    let destination = format!("{}:{}", config.destination().name(), config.destination().port());

    let (upgraded, server_public_key) = connect_rdcleanpath(
        &mut framed,
        &mut connector,
        destination,
        rdcleanpath.auth_token.clone(),
        None,
    )
    .await?;

    let connection_result = ironrdp_tokio::connect_finalize(
        upgraded,
        connector,
        &mut framed,
        &mut ReqwestNetworkClient::new(),
        config.destination().into(),
        server_public_key,
        None,
    )
    .await?;

    let (ws, leftover_bytes) = framed.into_inner();
    let erased_stream: Box<dyn AsyncReadWrite + Unpin + Send + Sync> = Box::new(ws);
    let upgraded_framed = ironrdp_tokio::TokioFramed::new_with_leftover(erased_stream, leftover_bytes);

    Ok((connection_result, upgraded_framed))
}

async fn connect_rdcleanpath<S>(
    framed: &mut ironrdp_tokio::Framed<S>,
    connector: &mut connector::ClientConnector,
    destination: String,
    proxy_auth_token: String,
    pcb: Option<String>,
) -> ConnectorResult<(ironrdp_tokio::Upgraded, Vec<u8>)>
where
    S: ironrdp_tokio::FramedRead + FramedWrite,
{
    use ironrdp::connector::Sequence as _;
    use x509_cert::der::Decode as _;

    #[derive(Clone, Copy, Debug)]
    struct RDCleanPathHint;

    // Use static to ensure 'static lifetime for the PduHint reference
    static RDCLEANPATH_HINT: RDCleanPathHint = RDCleanPathHint;

    impl ironrdp::pdu::PduHint for RDCleanPathHint {
        fn find_size(&self, bytes: &[u8]) -> ironrdp::core::DecodeResult<Option<(bool, usize)>> {
            match ironrdp_rdcleanpath::RDCleanPathPdu::detect(bytes) {
                ironrdp_rdcleanpath::DetectionResult::Detected { total_length, .. } => Ok(Some((true, total_length))),
                ironrdp_rdcleanpath::DetectionResult::NotEnoughBytes => Ok(None),
                ironrdp_rdcleanpath::DetectionResult::Failed => Err(ironrdp::core::other_err!(
                    "RDCleanPathHint",
                    "detection failed (invalid PDU)"
                )),
            }
        }
    }

    let mut buf = WriteBuf::new();

    info!("Begin connection procedure");

    {
        // RDCleanPath request

        let connector::ClientConnectorState::ConnectionInitiationSendRequest = connector.state else {
            return Err(connector::general_err!("invalid connector state (send request)"));
        };

        debug_assert!(connector.next_pdu_hint().is_none());

        let written = connector.step_no_input(&mut buf)?;
        let x224_pdu_len = written.size().expect("written size");
        debug_assert_eq!(x224_pdu_len, buf.filled_len());
        let x224_pdu = buf.filled().to_vec();

        let rdcleanpath_req =
            ironrdp_rdcleanpath::RDCleanPathPdu::new_request(x224_pdu, destination, proxy_auth_token, pcb)
                .map_err(|e| connector::custom_err!("new RDCleanPath request", e))?;
        debug!(message = ?rdcleanpath_req, "Send RDCleanPath request");
        let rdcleanpath_req = rdcleanpath_req
            .to_der()
            .map_err(|e| connector::custom_err!("RDCleanPath request encode", e))?;

        framed
            .write_all(&rdcleanpath_req)
            .await
            .map_err(|e| connector::custom_err!("couldn't write RDCleanPath request", e))?;
    }

    {
        // RDCleanPath response

        let rdcleanpath_res = framed
            .read_by_hint(&RDCLEANPATH_HINT)
            .await
            .map_err(|e| connector::custom_err!("read RDCleanPath request", e))?;

        let rdcleanpath_res = ironrdp_rdcleanpath::RDCleanPathPdu::from_der(&rdcleanpath_res)
            .map_err(|e| connector::custom_err!("RDCleanPath response decode", e))?;

        debug!(message = ?rdcleanpath_res, "Received RDCleanPath PDU");

        let (x224_connection_response, server_cert_chain) = match rdcleanpath_res
            .into_enum()
            .map_err(|e| connector::custom_err!("invalid RDCleanPath PDU", e))?
        {
            ironrdp_rdcleanpath::RDCleanPath::Request { .. } => {
                return Err(connector::general_err!(
                    "received an unexpected RDCleanPath type (request)",
                ));
            }
            ironrdp_rdcleanpath::RDCleanPath::Response {
                x224_connection_response,
                server_cert_chain,
                server_addr: _,
            } => (x224_connection_response, server_cert_chain),
            ironrdp_rdcleanpath::RDCleanPath::GeneralErr(error) => {
                return Err(connector::custom_err!("received an RDCleanPath error", error));
            }
            ironrdp_rdcleanpath::RDCleanPath::NegotiationErr {
                x224_connection_response,
            } => {
                // Try to decode as X.224 Connection Confirm to extract negotiation failure details.
                if let Ok(x224_confirm) = ironrdp_core::decode::<
                    ironrdp::pdu::x224::X224<ironrdp::pdu::nego::ConnectionConfirm>,
                >(&x224_connection_response)
                {
                    if let ironrdp::pdu::nego::ConnectionConfirm::Failure { code } = x224_confirm.0 {
                        // Convert to negotiation failure instead of generic RDCleanPath error.
                        let negotiation_failure = connector::NegotiationFailure::from(code);
                        return Err(connector::ConnectorError::new(
                            "RDP negotiation failed",
                            connector::ConnectorErrorKind::Negotiation(negotiation_failure),
                        ));
                    }
                }

                // Fallback to generic error if we can't decode the negotiation failure.
                return Err(connector::general_err!("received an RDCleanPath negotiation error"));
            }
        };

        let connector::ClientConnectorState::ConnectionInitiationWaitConfirm { .. } = connector.state else {
            return Err(connector::general_err!("invalid connector state (wait confirm)"));
        };

        debug_assert!(connector.next_pdu_hint().is_some());

        buf.clear();
        let written = connector.step(x224_connection_response.as_bytes(), &mut buf)?;

        debug_assert!(written.is_nothing());

        let server_cert = server_cert_chain
            .into_iter()
            .next()
            .ok_or_else(|| connector::general_err!("server cert chain missing from rdcleanpath response"))?;

        let cert = x509_cert::Certificate::from_der(server_cert.as_bytes())
            .map_err(|e| connector::custom_err!("server cert chain missing from rdcleanpath response", e))?;

        let server_public_key = cert
            .tbs_certificate
            .subject_public_key_info
            .subject_public_key
            .as_bytes()
            .ok_or_else(|| connector::general_err!("subject public key BIT STRING is not aligned"))?
            .to_owned();

        let should_upgrade = ironrdp_tokio::skip_connect_begin(connector);

        // At this point, proxy established the TLS session.

        let upgraded = ironrdp_tokio::mark_as_upgraded(should_upgrade, connector);

        Ok((upgraded, server_public_key))
    }
}

async fn active_session(
    session_id: SessionId,
    framed: UpgradedFramed,
    connection_result: ConnectionResult,
    event_loop_proxy: &EventLoopProxy<RdpOutputEvent>,
    input_event_receiver: &mut mpsc::UnboundedReceiver<RdpInputEvent>,
) -> SessionResult<RdpControlFlow> {
    let (mut reader, mut writer) = split_tokio_framed(framed);
    let mut image = DecodedImage::new(
        PixelFormat::RgbA32,
        connection_result.desktop_size.width,
        connection_result.desktop_size.height,
    );

    let mut active_stage = ActiveStage::new(connection_result);

    let disconnect_reason = 'outer: loop {
        let outputs = tokio::select! {
            frame = reader.read_pdu() => {
                let (action, payload) = frame.map_err(|e| session::custom_err!("read frame", e))?;
                trace!(?action, frame_length = payload.len(), "Frame received");

                active_stage.process(&mut image, action, &payload)?
            }
            input_event = input_event_receiver.recv() => {
                let input_event = input_event.ok_or_else(|| session::general_err!("GUI is stopped"))?;

                match input_event {
                    RdpInputEvent::Resize { width, height, scale_factor, physical_size } => {
                        trace!(width, height, "Resize event");
                        let width = u32::from(width);
                        let height = u32::from(height);
                        // TODO: Make adjust_display_size take and return width and height as u16.
                        // From the function's doc comment, the width and height values must be less than or equal to 8192 pixels.
                        // Therefore, we can remove unnecessary casts from u16 to u32 and back.
                        let (width, height) = MonitorLayoutEntry::adjust_display_size(width, height);
                        debug!(width, height, "Adjusted display size");
                        if let Some(response_frame) = active_stage.encode_resize(width, height, Some(scale_factor), physical_size) {
                            vec![ActiveStageOutput::ResponseFrame(response_frame?)]
                        } else {
                            // TODO(#271): use the "auto-reconnect cookie": https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-rdpbcgr/15b0d1c9-2891-4adb-a45e-deb4aeeeab7c
                            debug!("Reconnecting with new size");
                            let width = u16::try_from(width).expect("always in the range");
                            let height = u16::try_from(height).expect("always in the range");
                            return Ok(RdpControlFlow::ReconnectWithNewSize { width, height })
                        }
                    },
                    RdpInputEvent::FastPath(events) => {
                        trace!(?events);
                        active_stage.process_fastpath_input(&mut image, &events)?
                    }
                    RdpInputEvent::Close => {
                        active_stage.graceful_shutdown()?
                    }
                    RdpInputEvent::Clipboard(event) => {
                        if let Some(cliprdr) = active_stage.get_svc_processor::<cliprdr::CliprdrClient>() {
                            if let Some(svc_messages) = match event {
                                ClipboardMessage::SendInitiateCopy(formats) => {
                                    Some(cliprdr.initiate_copy(&formats)
                                        .map_err(|e| session::custom_err!("CLIPRDR", e))?)
                                }
                                ClipboardMessage::SendFormatData(response) => {
                                    Some(cliprdr.submit_format_data(response)
                                    .map_err(|e| session::custom_err!("CLIPRDR", e))?)
                                }
                                ClipboardMessage::SendInitiatePaste(format) => {
                                    Some(cliprdr.initiate_paste(format)
                                        .map_err(|e| session::custom_err!("CLIPRDR", e))?)
                                }
                                ClipboardMessage::Error(e) => {
                                    error!("Clipboard backend error: {}", e);
                                    None
                                }
                            } {
                                let frame = active_stage.process_svc_processor_messages(svc_messages)?;
                                // Send the messages to the server
                                vec![ActiveStageOutput::ResponseFrame(frame)]
                            } else {
                                // No messages to send to the server
                                Vec::new()
                            }
                        } else  {
                            warn!("Clipboard event received, but Cliprdr is not available");
                            Vec::new()
                        }
                    }
                    RdpInputEvent::SendDvcMessages { channel_id, messages } => {
                        trace!(channel_id, ?messages, "Send DVC messages");

                        let frame = active_stage.encode_dvc_messages(messages)?;
                        vec![ActiveStageOutput::ResponseFrame(frame)]
                    }
                }
            }
        };

        for out in outputs {
            match out {
                ActiveStageOutput::ResponseFrame(frame) => writer
                    .write_all(&frame)
                    .await
                    .map_err(|e| session::custom_err!("write response", e))?,
                ActiveStageOutput::GraphicsUpdate(_region) => {
                    let buffer: Vec<u32> = image
                        .data()
                        .chunks_exact(4)
                        .map(|pixel| {
                            let r = pixel[0];
                            let g = pixel[1];
                            let b = pixel[2];
                            u32::from_be_bytes([0, r, g, b])
                        })
                        .collect();

                    event_loop_proxy
                        .send_event(RdpOutputEvent::Image {
                            session_id,
                            buffer,
                            width: NonZeroU16::new(image.width())
                                .ok_or_else(|| session::general_err!("width is zero"))?,
                            height: NonZeroU16::new(image.height())
                                .ok_or_else(|| session::general_err!("height is zero"))?,
                        })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::PointerDefault => {
                    event_loop_proxy
                        .send_event(RdpOutputEvent::PointerDefault { session_id })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::PointerHidden => {
                    event_loop_proxy
                        .send_event(RdpOutputEvent::PointerHidden { session_id })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::PointerPosition { x, y } => {
                    event_loop_proxy
                        .send_event(RdpOutputEvent::PointerPosition { session_id, x, y })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::PointerBitmap(pointer) => {
                    event_loop_proxy
                        .send_event(RdpOutputEvent::PointerBitmap { session_id, pointer })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::DeactivateAll(mut connection_activation) => {
                    // Execute the Deactivation-Reactivation Sequence:
                    // https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-rdpbcgr/dfc234ce-481a-4674-9a5d-2a7bafb14432
                    debug!("Received Server Deactivate All PDU, executing Deactivation-Reactivation Sequence");
                    let mut buf = WriteBuf::new();
                    'activation_seq: loop {
                        let written = single_sequence_step_read(&mut reader, &mut *connection_activation, &mut buf)
                            .await
                            .map_err(|e| session::custom_err!("read deactivation-reactivation sequence step", e))?;

                        if written.size().is_some() {
                            writer.write_all(buf.filled()).await.map_err(|e| {
                                session::custom_err!("write deactivation-reactivation sequence step", e)
                            })?;
                        }

                        if let ConnectionActivationState::Finalized {
                            io_channel_id,
                            user_channel_id,
                            desktop_size,
                            enable_server_pointer,
                            pointer_software_rendering,
                        } = connection_activation.connection_activation_state()
                        {
                            debug!(?desktop_size, "Deactivation-Reactivation Sequence completed");
                            // Update image size with the new desktop size.
                            image = DecodedImage::new(PixelFormat::RgbA32, desktop_size.width, desktop_size.height);
                            // Update the active stage with the new channel IDs and pointer settings.
                            active_stage.set_fastpath_processor(
                                fast_path::ProcessorBuilder {
                                    io_channel_id,
                                    user_channel_id,
                                    enable_server_pointer,
                                    pointer_software_rendering,
                                }
                                .build(),
                            );
                            active_stage.set_enable_server_pointer(enable_server_pointer);
                            break 'activation_seq;
                        }
                    }
                }
                ActiveStageOutput::Terminate(reason) => break 'outer reason,
            }
        }
    };

    Ok(RdpControlFlow::TerminatedGracefully(disconnect_reason))
}

// ============================================================================
// Multi-session support functions
// ============================================================================

/// Connect to a specific destination (for multi-session mode)
async fn connect_for_destination(
    config: Config,
    destination: Destination,
    cliprdr_factory: Option<Arc<std::sync::Mutex<Box<dyn CliprdrBackendFactory + Send>>>>,
    dvc_pipe_proxy_factory: DvcPipeProxyFactory,
) -> ConnectorResult<(ConnectionResult, UpgradedFramed)> {
    let dest = format!("{}:{}", destination.name(), destination.port());

    let stream = TcpStream::connect(&dest)
        .await
        .map_err(|e| connector::custom_err!("TCP connect", e))?;
    let client_addr = stream
        .local_addr()
        .map_err(|e| connector::custom_err!("get socket local address", e))?;
    let mut framed = ironrdp_tokio::TokioFramed::new(stream);

    let mut drdynvc =
        ironrdp::dvc::DrdynvcClient::new().with_dynamic_channel(DisplayControlClient::new(|_| Ok(Vec::new())));

    // Instantiate all DVC proxies
    for proxy in config.dvc_pipe_proxies.iter() {
        let channel_name = proxy.channel_name.clone();
        let pipe_name = proxy.pipe_name.clone();

        trace!(%channel_name, %pipe_name, "Creating DVC proxy");

        drdynvc = drdynvc.with_dynamic_channel(dvc_pipe_proxy_factory.create(channel_name, pipe_name));
    }

    let mut connector = connector::ClientConnector::new(config.connector.clone(), client_addr)
        .with_static_channel(drdynvc)
        .with_static_channel(rdpsnd::client::Rdpsnd::new(Box::new(cpal::RdpsndBackend::new())))
        .with_static_channel(rdpdr::Rdpdr::new(Box::new(NoopRdpdrBackend {}), "IronRDP".to_owned()).with_smartcard(0));

    if let Some(factory_mutex) = cliprdr_factory.as_ref() {
        let factory = factory_mutex.lock().unwrap();
        let backend = factory.build_cliprdr_backend();

        let cliprdr = cliprdr::Cliprdr::new(backend);

        connector.attach_static_channel(cliprdr);
    }

    let should_upgrade = ironrdp_tokio::connect_begin(&mut framed, &mut connector).await?;

    debug!("TLS upgrade");

    // Ensure there is no leftover
    let (initial_stream, leftover_bytes) = framed.into_inner();

    let (upgraded_stream, tls_cert) = ironrdp_tls::upgrade(initial_stream, destination.name())
        .await
        .map_err(|e| connector::custom_err!("TLS upgrade", e))?;

    let upgraded = ironrdp_tokio::mark_as_upgraded(should_upgrade, &mut connector);

    let erased_stream: Box<dyn AsyncReadWrite + Unpin + Send + Sync> = Box::new(upgraded_stream);
    let mut upgraded_framed = ironrdp_tokio::TokioFramed::new_with_leftover(erased_stream, leftover_bytes);

    let server_public_key = ironrdp_tls::extract_tls_server_public_key(&tls_cert)
        .ok_or_else(|| connector::general_err!("unable to extract tls server public key"))?;
    let connection_result = ironrdp_tokio::connect_finalize(
        upgraded,
        connector,
        &mut upgraded_framed,
        &mut ReqwestNetworkClient::new(),
        destination.into(),
        server_public_key.to_owned(),
        None,
    )
    .await?;

    debug!(?connection_result);

    Ok((connection_result, upgraded_framed))
}

/// Connect via WebSocket to a specific destination (for multi-session mode)
async fn connect_ws_for_destination(
    config: Config,
    destination: Destination,
    rdcleanpath: RDCleanPathConfig,
    cliprdr_factory: Option<Arc<std::sync::Mutex<Box<dyn CliprdrBackendFactory + Send>>>>,
    dvc_pipe_proxy_factory: DvcPipeProxyFactory,
) -> ConnectorResult<(ConnectionResult, UpgradedFramed)> {
    let hostname = rdcleanpath
        .url
        .host_str()
        .ok_or_else(|| connector::general_err!("host missing from the URL"))?;

    let port = rdcleanpath.url.port_or_known_default().unwrap_or(443);

    let socket = TcpStream::connect((hostname, port))
        .await
        .map_err(|e| connector::custom_err!("TCP connect", e))?;

    socket
        .set_nodelay(true)
        .map_err(|e| connector::custom_err!("set TCP_NODELAY", e))?;

    let client_addr = socket
        .local_addr()
        .map_err(|e| connector::custom_err!("get socket local address", e))?;

    let (ws, _) = tokio_tungstenite::client_async_tls(rdcleanpath.url.as_str(), socket)
        .await
        .map_err(|e| connector::custom_err!("WS connect", e))?;

    let ws = crate::ws::websocket_compat(ws);

    let mut framed = ironrdp_tokio::TokioFramed::new(ws);

    let mut drdynvc =
        ironrdp::dvc::DrdynvcClient::new().with_dynamic_channel(DisplayControlClient::new(|_| Ok(Vec::new())));

    // Instantiate all DVC proxies
    for proxy in config.dvc_pipe_proxies.iter() {
        let channel_name = proxy.channel_name.clone();
        let pipe_name = proxy.pipe_name.clone();

        trace!(%channel_name, %pipe_name, "Creating DVC proxy");

        drdynvc = drdynvc.with_dynamic_channel(dvc_pipe_proxy_factory.create(channel_name, pipe_name));
    }

    let mut connector = connector::ClientConnector::new(config.connector.clone(), client_addr)
        .with_static_channel(drdynvc)
        .with_static_channel(rdpsnd::client::Rdpsnd::new(Box::new(cpal::RdpsndBackend::new())))
        .with_static_channel(rdpdr::Rdpdr::new(Box::new(NoopRdpdrBackend {}), "IronRDP".to_owned()).with_smartcard(0));

    if let Some(factory_mutex) = cliprdr_factory.as_ref() {
        let factory = factory_mutex.lock().unwrap();
        let backend = factory.build_cliprdr_backend();

        let cliprdr = cliprdr::Cliprdr::new(backend);

        connector.attach_static_channel(cliprdr);
    }

    let dest_str = format!("{}:{}", destination.name(), destination.port());

    let (upgraded, server_public_key) = connect_rdcleanpath(
        &mut framed,
        &mut connector,
        dest_str,
        rdcleanpath.auth_token.clone(),
        None,
    )
    .await?;

    let connection_result = ironrdp_tokio::connect_finalize(
        upgraded,
        connector,
        &mut framed,
        &mut ReqwestNetworkClient::new(),
        (&destination).into(),
        server_public_key,
        None,
    )
    .await?;

    let (ws, leftover_bytes) = framed.into_inner();
    let erased_stream: Box<dyn AsyncReadWrite + Unpin + Send + Sync> = Box::new(ws);
    let upgraded_framed = ironrdp_tokio::TokioFramed::new_with_leftover(erased_stream, leftover_bytes);

    Ok((connection_result, upgraded_framed))
}

/// Active session loop for a multi-session task (uses BroadcastEvent from broadcast channel)
async fn active_session_for_task(
    session_id: SessionId,
    framed: UpgradedFramed,
    connection_result: ConnectionResult,
    event_loop_proxy: EventLoopProxy<RdpOutputEvent>,
    mut broadcast_receiver: broadcast::Receiver<BroadcastEvent>,
) -> (SessionResult<SessionControlFlow>, broadcast::Receiver<BroadcastEvent>) {
    // Use a helper function that returns Result, then wrap the result with the receiver
    async fn inner(
        session_id: SessionId,
        framed: UpgradedFramed,
        connection_result: ConnectionResult,
        event_loop_proxy: EventLoopProxy<RdpOutputEvent>,
        broadcast_receiver: &mut broadcast::Receiver<BroadcastEvent>,
    ) -> SessionResult<SessionControlFlow> {
    let (mut reader, mut writer) = split_tokio_framed(framed);
    let mut image = DecodedImage::new(
        PixelFormat::RgbA32,
        connection_result.desktop_size.width,
        connection_result.desktop_size.height,
    );

    let mut active_stage = ActiveStage::new(connection_result);

    let disconnect_reason = 'outer: loop {
        let outputs = tokio::select! {
            frame = reader.read_pdu() => {
                let (action, payload) = frame.map_err(|e| session::custom_err!("read frame", e))?;
                trace!(?action, frame_length = payload.len(), "Frame received");

                active_stage.process(&mut image, action, &payload)?
            }
            input_event = broadcast_receiver.recv() => {
                let input_event = match input_event {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(session::general_err!("Coordinator stopped"));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(session_id = ?session_id, lagged = n, "Session lagged behind, missed events");
                        continue;
                    }
                };

                match input_event {
                    BroadcastEvent::Resize(resize) => {
                        trace!(resize.width, resize.height, "Resize event");
                        let width = u32::from(resize.width);
                        let height = u32::from(resize.height);
                        let (width, height) = MonitorLayoutEntry::adjust_display_size(width, height);
                        debug!(width, height, "Adjusted display size");
                        if let Some(response_frame) = active_stage.encode_resize(width, height, Some(resize.scale_factor), resize.physical_size) {
                            vec![ActiveStageOutput::ResponseFrame(response_frame?)]
                        } else {
                            debug!("Reconnecting with new size");
                            let width = u16::try_from(width).expect("always in the range");
                            let height = u16::try_from(height).expect("always in the range");
                            return Ok(SessionControlFlow::ReconnectWithNewSize { width, height })
                        }
                    },
                    BroadcastEvent::FastPath(events) => {
                        trace!(?events);
                        active_stage.process_fastpath_input(&mut image, &events)?
                    }
                    BroadcastEvent::Close => {
                        active_stage.graceful_shutdown()?
                    }
                    BroadcastEvent::Clipboard(event) => {
                        if let Some(cliprdr) = active_stage.get_svc_processor::<cliprdr::CliprdrClient>() {
                            if let Some(svc_messages) = match event.as_ref() {
                                ClipboardMessage::SendInitiateCopy(formats) => {
                                    Some(cliprdr.initiate_copy(formats)
                                        .map_err(|e| session::custom_err!("CLIPRDR", e))?)
                                }
                                ClipboardMessage::SendFormatData(response) => {
                                    Some(cliprdr.submit_format_data(response.clone())
                                        .map_err(|e| session::custom_err!("CLIPRDR", e))?)
                                }
                                ClipboardMessage::SendInitiatePaste(format) => {
                                    Some(cliprdr.initiate_paste(*format)
                                        .map_err(|e| session::custom_err!("CLIPRDR", e))?)
                                }
                                ClipboardMessage::Error(e) => {
                                    error!("Clipboard backend error: {}", e);
                                    None
                                }
                            } {
                                let frame = active_stage.process_svc_processor_messages(svc_messages)?;
                                vec![ActiveStageOutput::ResponseFrame(frame)]
                            } else {
                                Vec::new()
                            }
                        } else {
                            trace!("Clipboard event received, but Cliprdr is not available for this session");
                            Vec::new()
                        }
                    }
                    BroadcastEvent::DvcMessages { channel_id, messages } => {
                        // Take the messages (only first session to receive will get them)
                        if let Some(msgs) = messages.lock().unwrap().take() {
                            trace!(channel_id, num_messages = msgs.len(), "Send DVC messages");
                            let frame = active_stage.encode_dvc_messages(msgs)?;
                            vec![ActiveStageOutput::ResponseFrame(frame)]
                        } else {
                            // Another session already took the messages
                            Vec::new()
                        }
                    }
                }
            }
        };

        for out in outputs {
            match out {
                ActiveStageOutput::ResponseFrame(frame) => writer
                    .write_all(&frame)
                    .await
                    .map_err(|e| session::custom_err!("write response", e))?,
                ActiveStageOutput::GraphicsUpdate(_region) => {
                    let buffer: Vec<u32> = image
                        .data()
                        .chunks_exact(4)
                        .map(|pixel| {
                            let r = pixel[0];
                            let g = pixel[1];
                            let b = pixel[2];
                            u32::from_be_bytes([0, r, g, b])
                        })
                        .collect();

                    event_loop_proxy
                        .send_event(RdpOutputEvent::Image {
                            session_id,
                            buffer,
                            width: NonZeroU16::new(image.width())
                                .ok_or_else(|| session::general_err!("width is zero"))?,
                            height: NonZeroU16::new(image.height())
                                .ok_or_else(|| session::general_err!("height is zero"))?,
                        })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::PointerDefault => {
                    event_loop_proxy
                        .send_event(RdpOutputEvent::PointerDefault { session_id })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::PointerHidden => {
                    event_loop_proxy
                        .send_event(RdpOutputEvent::PointerHidden { session_id })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::PointerPosition { x, y } => {
                    event_loop_proxy
                        .send_event(RdpOutputEvent::PointerPosition { session_id, x, y })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::PointerBitmap(pointer) => {
                    event_loop_proxy
                        .send_event(RdpOutputEvent::PointerBitmap { session_id, pointer })
                        .map_err(|e| session::custom_err!("event_loop_proxy", e))?;
                }
                ActiveStageOutput::DeactivateAll(mut connection_activation) => {
                    debug!("Received Server Deactivate All PDU, executing Deactivation-Reactivation Sequence");
                    let mut buf = WriteBuf::new();
                    'activation_seq: loop {
                        let written = single_sequence_step_read(&mut reader, &mut *connection_activation, &mut buf)
                            .await
                            .map_err(|e| session::custom_err!("read deactivation-reactivation sequence step", e))?;

                        if written.size().is_some() {
                            writer.write_all(buf.filled()).await.map_err(|e| {
                                session::custom_err!("write deactivation-reactivation sequence step", e)
                            })?;
                        }

                        if let ConnectionActivationState::Finalized {
                            io_channel_id,
                            user_channel_id,
                            desktop_size,
                            enable_server_pointer,
                            pointer_software_rendering,
                        } = connection_activation.connection_activation_state()
                        {
                            debug!(?desktop_size, "Deactivation-Reactivation Sequence completed");
                            image = DecodedImage::new(PixelFormat::RgbA32, desktop_size.width, desktop_size.height);
                            active_stage.set_fastpath_processor(
                                fast_path::ProcessorBuilder {
                                    io_channel_id,
                                    user_channel_id,
                                    enable_server_pointer,
                                    pointer_software_rendering,
                                }
                                .build(),
                            );
                            active_stage.set_enable_server_pointer(enable_server_pointer);
                            break 'activation_seq;
                        }
                    }
                }
                ActiveStageOutput::Terminate(reason) => break 'outer reason,
            }
        }
    };

    Ok(SessionControlFlow::TerminatedGracefully(disconnect_reason))
    }
    
    // Call the inner function and pair the result with the receiver
    let result = inner(
        session_id,
        framed,
        connection_result,
        event_loop_proxy,
        &mut broadcast_receiver,
    ).await;
    (result, broadcast_receiver)
}
