// SPDX-License-Identifier: Apache-2.0
//! Fixed loopback transport. The socket owns admission throughout TLS and HTTP/2.

use super::{
    attempts::{Authentication, Connection, Principal},
    *,
};
use futures::{Stream, stream};
use std::{
    io::{self, Read as _},
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tonic::{
    Request, Status,
    transport::{
        Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
        server::{Connected, TcpConnectInfo, TlsConnectInfo},
    },
};

pub(crate) const IDLE: Duration = Duration::from_secs(5);

pub(crate) fn verify_certificate_manifest(root: &Path, expected_sha256: &str) -> Result<()> {
    use rustls::pki_types::{CertificateDer, pem::PemObject as _};
    #[derive(serde::Deserialize)]
    struct Manifest {
        certificates: Vec<CertificateRecord>,
    }
    #[derive(serde::Deserialize)]
    struct CertificateRecord {
        name: String,
        filename: String,
        der_sha256: String,
    }
    let bytes = read_pem(&root.join("public-manifest.json"))?;
    if hash(&bytes) != authority::unhex(expected_sha256)? {
        return Err(mismatch(
            "certificate allowlist differs from supervised configuration",
        ));
    }
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|_| invalid("invalid certificate manifest"))?;
    let roles = [
        "ca",
        "server",
        "reader",
        "reader2",
        "denied",
        "administrator",
        "unlisted",
        "expired",
        "unknown",
    ];
    if manifest.certificates.len() != roles.len() {
        return Err(invalid("unexpected certificate manifest size"));
    }
    for role in roles {
        let matching = manifest
            .certificates
            .iter()
            .filter(|entry| entry.name == role)
            .collect::<Vec<_>>();
        if matching.len() != 1 || matching[0].filename != format!("{role}.cert.pem") {
            return Err(invalid(
                "certificate manifest requires each fixed role once",
            ));
        }
        let pem = read_pem(&root.join(&matching[0].filename))?;
        let mut certificates = CertificateDer::pem_slice_iter(&pem);
        let certificate = certificates
            .next()
            .ok_or_else(|| invalid("missing PEM certificate"))?
            .map_err(|_| invalid("malformed PEM certificate"))?;
        if certificates.next().is_some()
            || hash(certificate.as_ref()) != authority::unhex(&matching[0].der_sha256)?
        {
            return Err(mismatch(
                "local certificate differs from fixed role identity",
            ));
        }
    }
    Ok(())
}

pub(crate) fn authentication(root: &Path) -> Result<Arc<Authentication>> {
    #[derive(serde::Deserialize)]
    struct Manifest {
        certificates: Vec<CertificateRecord>,
    }
    #[derive(serde::Deserialize)]
    struct CertificateRecord {
        name: String,
        der_sha256: String,
    }
    let manifest: Manifest = serde_json::from_slice(&read_pem(&root.join("public-manifest.json"))?)
        .map_err(|_| invalid("invalid local certificate allowlist"))?;
    let mut fingerprints = Vec::with_capacity(4);
    for role in ["reader", "reader2", "denied", "administrator"] {
        let mut matching = manifest
            .certificates
            .iter()
            .filter(|certificate| certificate.name == role);
        let certificate = matching
            .next()
            .ok_or_else(|| invalid("certificate role missing from allowlist"))?;
        if matching.next().is_some() {
            return Err(invalid("duplicate certificate allowlist role"));
        }
        fingerprints.push(authority::unhex(&certificate.der_sha256)?);
    }
    Ok(Arc::new(Authentication::new(
        fingerprints
            .try_into()
            .map_err(|_| invalid("invalid certificate allowlist size"))?,
    )?))
}

pub(crate) fn status(error: Error) -> Status {
    let message = format!("{}: {}", error.code, error.detail);
    match error.code {
        "SLX_UNAUTHENTICATED" => Status::unauthenticated(message),
        "SLX_PERMISSION" => Status::permission_denied(message),
        "SLX_RESOURCE_LIMIT" => Status::resource_exhausted(message),
        "SLX_CANCELLED" => Status::cancelled(message),
        "SLX_IDLE" => Status::deadline_exceeded(message),
        "SLX_INTERNAL" => Status::internal(message),
        _ => Status::invalid_argument(message),
    }
}

pub(crate) struct Pem {
    ca: Vec<u8>,
    certificate: Vec<u8>,
    private_key: Vec<u8>,
}

fn read_pem(path: &Path) -> Result<Vec<u8>> {
    let input = std::fs::File::open(path).map_err(|_| invalid("cannot open local TLS fixture"))?;
    let mut bytes = Vec::new();
    input
        .take((64 << 10) + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read local TLS fixture"))?;
    if bytes.is_empty() || bytes.len() > 64 << 10 {
        return Err(limit("TLS fixture length is outside its bound"));
    }
    Ok(bytes)
}

impl Pem {
    pub(crate) fn load(root: &Path, role: &str) -> Result<Self> {
        if ![
            "server",
            "reader",
            "reader2",
            "denied",
            "administrator",
            "expired",
            "unknown",
            "unlisted",
        ]
        .contains(&role)
        {
            return Err(invalid("unknown TLS fixture role"));
        }
        Ok(Self {
            ca: read_pem(&root.join("ca.cert.pem"))?,
            certificate: read_pem(&root.join(format!("{role}.cert.pem")))?,
            private_key: read_pem(&root.join(format!("{role}.key.pem")))?,
        })
    }

    fn identity(&self) -> Identity {
        Identity::from_pem(&self.certificate, &self.private_key)
    }
}

pub(crate) fn server(pem: &Pem) -> Result<Server> {
    Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(pem.identity())
                .client_ca_root(Certificate::from_pem(&pem.ca))
                .client_auth_optional(false)
                .timeout(IDLE),
        )
        .map_err(|_| invalid("server TLS configuration is invalid"))
        .map(|server| {
            server
                .concurrency_limit_per_connection(2)
                .max_concurrent_streams(2)
                .initial_stream_window_size(1 << 20)
                .initial_connection_window_size(2 << 20)
                .http2_adaptive_window(Some(false))
                .http2_max_header_list_size(16 << 10)
                .max_frame_size(16 << 10)
        })
}

pub(crate) fn endpoint(
    address: SocketAddr,
    pem: &Pem,
    server_name: &str,
    with_identity: bool,
) -> Result<Endpoint> {
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err(invalid("exchange endpoint must be concrete loopback"));
    }
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&pem.ca))
        .domain_name(server_name)
        .timeout(IDLE);
    let tls = if with_identity {
        tls.identity(pem.identity())
    } else {
        tls
    };
    Endpoint::from_shared(format!("https://{address}"))
        .map_err(|_| invalid("invalid loopback endpoint"))?
        .tls_config(tls)
        .map_err(|_| invalid("client TLS configuration is invalid"))
        .map(|endpoint| {
            endpoint
                .connect_timeout(IDLE)
                .concurrency_limit(2)
                .buffer_size(2)
                .initial_stream_window_size(1 << 20)
                .initial_connection_window_size(2 << 20)
                .http2_adaptive_window(false)
                .http2_max_header_list_size(16 << 10)
                .max_frame_size(16 << 10)
        })
}

#[derive(Clone)]
pub(crate) struct Call {
    pub(crate) principal: Principal,
    pub(crate) connection: Connection,
    pub(crate) local: SocketAddr,
    pub(crate) remote: SocketAddr,
}

#[derive(Clone)]
pub(crate) struct ConnectInfo {
    tcp: TcpConnectInfo,
    data: Connection,
}

pub(crate) fn authenticate(
    authentication: Arc<Authentication>,
) -> impl FnMut(Request<()>) -> std::result::Result<Request<()>, Status> + Clone {
    move |mut request| {
        // With custom IO, Tonic preserves this exact generic connection type.
        // Request::peer_certs() only looks up its built-in TcpConnectInfo type.
        let info = request
            .extensions()
            .get::<TlsConnectInfo<ConnectInfo>>()
            .ok_or_else(|| {
                Status::unauthenticated("SLX_UNAUTHENTICATED: verified TLS connection required")
            })?;
        let certificates = info.peer_certs().ok_or_else(|| {
            Status::unauthenticated("SLX_UNAUTHENTICATED: client certificate required")
        })?;
        let leaf = certificates.first().ok_or_else(|| {
            Status::unauthenticated("SLX_UNAUTHENTICATED: empty peer certificate chain")
        })?;
        let principal = authentication
            .verified_leaf(leaf.as_ref())
            .map_err(status)?;
        let connection = info.get_ref();
        let local = connection
            .tcp
            .local_addr
            .ok_or_else(|| Status::internal("SLX_INTERNAL: local socket identity missing"))?;
        let remote = connection
            .tcp
            .remote_addr
            .ok_or_else(|| Status::internal("SLX_INTERNAL: peer socket identity missing"))?;
        if !local.ip().is_loopback() || !remote.ip().is_loopback() {
            return Err(Status::permission_denied(
                "SLX_PERMISSION: experiment requires loopback sockets",
            ));
        }
        let call = Call {
            principal,
            connection: connection.data.clone(),
            local,
            remote,
        };
        request.extensions_mut().insert(call);
        Ok(request)
    }
}

pub(crate) fn call<T>(request: &Request<T>) -> std::result::Result<Call, Status> {
    request.extensions().get::<Call>().cloned().ok_or_else(|| {
        Status::unauthenticated("SLX_UNAUTHENTICATED: request was not authenticated")
    })
}

pub(crate) fn control(
    registry: &attempts::Registry,
    call: &Call,
    action: &arrow_flight::Action,
) -> std::result::Result<Bytes, Status> {
    if action.encoded_len() > MAX_CONTROL {
        return Err(status(limit("control action exceeds its byte limit")));
    }
    match action.r#type.as_str() {
        "shardloom.cancel.v1" => {
            let cancel: wire::Cancel =
                strict_decode(action.body.clone(), MAX_CONTROL).map_err(status)?;
            registry
                .cancel(
                    call.principal,
                    cancel.binding.as_ref().ok_or_else(|| {
                        status(invalid("cancellation requires operation binding"))
                    })?,
                )
                .map_err(status)?;
        }
        "shardloom.replace-policy.v1" => {
            let replace: wire::ReplacePolicy =
                strict_decode(action.body.clone(), MAX_CONTROL).map_err(status)?;
            registry
                .replace_policy(call.principal, &replace)
                .map_err(status)?;
        }
        _ => {
            return Err(Status::unimplemented(
                "SLX_UNSUPPORTED: action is outside the finite exchange contract",
            ));
        }
    }
    Ok(Bytes::new())
}

#[derive(Clone)]
pub(crate) struct Admission(Arc<AdmissionState>);
struct AdmissionState {
    permits: Arc<Semaphore>,
    live: AtomicUsize,
    peak: AtomicUsize,
    accepted: AtomicUsize,
}

impl Admission {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AdmissionState {
            permits: Arc::new(Semaphore::new(2)),
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            accepted: AtomicUsize::new(0),
        }))
    }
    pub(crate) fn snapshot(&self) -> (usize, usize) {
        (
            self.0.live.load(Ordering::Acquire),
            self.0.peak.load(Ordering::Acquire),
        )
    }
    pub(crate) fn accepted(&self) -> usize {
        self.0.accepted.load(Ordering::Acquire)
    }
}

pub(crate) struct LimitedIo {
    socket: TcpStream,
    info: ConnectInfo,
    admission: Admission,
    _permit: OwnedSemaphorePermit,
}

impl Connected for LimitedIo {
    type ConnectInfo = ConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl Drop for LimitedIo {
    fn drop(&mut self) {
        self.admission.0.live.fetch_sub(1, Ordering::AcqRel);
    }
}

impl AsyncRead for LimitedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.info.data.readable(context) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_read(context, buffer)
    }
}

impl AsyncWrite for LimitedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.info.data.writable(context) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_write(context, bytes)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if !self.info.data.writable(context) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_write_vectored(context, buffers)
    }
    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.info.data.writable(context) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_flush(context)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(context)
    }
}

pub(crate) fn incoming(
    listener: TcpListener,
    admission: Admission,
) -> Pin<Box<dyn Stream<Item = io::Result<LimitedIo>> + Send>> {
    Box::pin(stream::unfold(
        (listener, admission),
        |(listener, admission)| async move {
            let Ok(permit) = admission.0.permits.clone().acquire_owned().await else {
                return None;
            };
            // No accept, TLS handshake or HTTP/2 connection exists without a permit.
            let accepted = listener.accept().await.and_then(|(socket, _)| {
                socket.set_nodelay(true)?;
                let info = ConnectInfo {
                    tcp: socket.connect_info(),
                    data: Connection::new(),
                };
                let live = admission.0.live.fetch_add(1, Ordering::AcqRel) + 1;
                admission.0.accepted.fetch_add(1, Ordering::AcqRel);
                admission.0.peak.fetch_max(live, Ordering::AcqRel);
                Ok(LimitedIo {
                    socket,
                    info,
                    admission: admission.clone(),
                    _permit: permit,
                })
            });
            Some((accepted, (listener, admission)))
        },
    ))
}

#[cfg(test)]
#[path = "native_exchange_transport_tests.rs"]
mod tests;
