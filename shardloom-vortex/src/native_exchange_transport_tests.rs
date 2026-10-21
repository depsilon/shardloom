// SPDX-License-Identifier: Apache-2.0
use super::super::{
    attempts::Registry,
    authority::{Authority, unhex},
};
use super::*;
use arrow_flight::{
    Action,
    flight_service_client::FlightServiceClient,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{SqlInfo, server::FlightSqlService},
};
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tonic::{Code, Response, service::interceptor::InterceptedService, transport::Channel};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn socket_admission_is_retained_before_tls_and_across_io_ownership() {
    runtime().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let admission = Admission::new();
        let mut incoming = incoming(listener, admission.clone());
        let mut clients = Vec::new();
        let mut held = Vec::new();
        for _ in 0..2 {
            clients.push(TcpStream::connect(address).await.unwrap());
            held.push(incoming.next().await.unwrap().unwrap());
        }
        let third_client = TcpStream::connect(address).await.unwrap();
        assert!(incoming.next().now_or_never().is_none());
        assert_eq!(admission.snapshot(), (2, 2));
        clients[0].write_all(b"q").await.unwrap();
        let mut byte = [0];
        held[0].read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"q");
        held[0].write_all(b"a").await.unwrap();
        held[0].flush().await.unwrap();
        clients[0].read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"a");
        let info = held[0].connect_info();
        assert_eq!(info.tcp.local_addr, Some(address));
        assert!(info.tcp.remote_addr.unwrap().ip().is_loopback());
        held[0].shutdown().await.unwrap();
        drop(held.remove(0));
        held.push(
            tokio::time::timeout(IDLE, incoming.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
        assert_eq!(admission.snapshot(), (2, 2));
        drop((held, clients, third_client, incoming));
        assert_eq!(admission.snapshot(), (0, 2));
    });
    let auth =
        Arc::new(Authentication::new([hash(b"a"), hash(b"b"), hash(b"c"), hash(b"d")]).unwrap());
    assert_eq!(
        authenticate(auth)(Request::new(())).unwrap_err().code(),
        Code::Unauthenticated
    );
}

// This service isolates the real transport/interceptor and control handlers.
// It supplies no payload and is not the two-process execution acceptance.
struct ControlProbe {
    registry: Registry,
}

#[tonic::async_trait]
impl FlightSqlService for ControlProbe {
    type FlightService = Self;
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let call = call(&request)?;
        assert!(call.local.ip().is_loopback() && call.remote.ip().is_loopback());
        // Read the connection-specific owner preserved through Tonic middleware.
        let _connection = call.connection.clone();
        let bytes = if request.get_ref().r#type == "test.issue-sql-ticket" {
            wire::SqlTicket {
                binding: Some(self.registry.issue_sql(call.principal).map_err(status)?),
            }
            .encode_to_vec()
            .into()
        } else {
            control(&self.registry, &call, request.get_ref())?
        };
        Ok(Response::new(Box::pin(stream::iter([Ok(
            arrow_flight::Result { body: bytes },
        )]))))
    }
}

async fn client(
    root: &Path,
    role: &str,
    address: SocketAddr,
    name: &str,
    identity: bool,
) -> std::result::Result<FlightServiceClient<Channel>, Status> {
    let pem = Pem::load(root, role).map_err(status)?;
    let channel = endpoint(address, &pem, name, identity)
        .map_err(status)?
        .connect()
        .await
        .map_err(|error| Status::unavailable(format!("test TLS connection failed: {error:?}")))?;
    Ok(FlightServiceClient::new(channel)
        .max_decoding_message_size(MAX_RPC)
        .max_encoding_message_size(MAX_RPC))
}

async fn action(
    client: &mut FlightServiceClient<Channel>,
    kind: &str,
    body: Bytes,
) -> std::result::Result<Bytes, Status> {
    let mut request = Request::new(Action {
        r#type: kind.to_owned(),
        body,
    });
    request.set_timeout(IDLE);
    // A forged header cannot override the certificate principal on a reused connection.
    request
        .metadata_mut()
        .insert("x-principal", "administrator".parse().unwrap());
    let mut response = client.do_action(request).await?.into_inner();
    let first = response
        .try_next()
        .await?
        .ok_or_else(|| Status::internal("missing action response"))?;
    if response.try_next().await?.is_some() {
        return Err(Status::internal("extra action response"));
    }
    Ok(first.body)
}

async fn ticket(client: &mut FlightServiceClient<Channel>) -> std::result::Result<Binding, Status> {
    let body = action(client, "test.issue-sql-ticket", Bytes::new()).await?;
    strict_decode::<wire::SqlTicket>(body, MAX_CONTROL)
        .map_err(status)?
        .binding
        .ok_or_else(|| Status::internal("missing ticket binding"))
}

async fn drained(admission: &Admission, live: usize) {
    tokio::time::timeout(IDLE, async {
        while admission.snapshot().0 != live {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

fn authentication(root: &Path) -> Arc<Authentication> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("public-manifest.json")).unwrap()).unwrap();
    let certificates = manifest["certificates"].as_array().unwrap();
    let fingerprints = ["reader", "reader2", "denied", "administrator"].map(|name| {
        unhex(
            certificates
                .iter()
                .find(|cert| cert["name"] == name)
                .unwrap()["der_sha256"]
                .as_str()
                .unwrap(),
        )
        .unwrap()
    });
    Arc::new(Authentication::new(fingerprints).unwrap())
}

async fn permissions(root: &Path, address: SocketAddr, admission: &Admission) {
    let mut reader = client(root, "reader", address, "localhost", true)
        .await
        .unwrap();
    let original = ticket(&mut reader).await.unwrap();
    assert_eq!(original.principal, "reader");
    let mut reader2 = client(root, "reader2", address, "localhost", true)
        .await
        .unwrap();
    let mut stolen = original.clone();
    stolen.principal = "reader2".into();
    let cancel = wire::Cancel {
        binding: Some(stolen),
    }
    .encode_to_vec()
    .into();
    assert!(
        action(&mut reader2, "shardloom.cancel.v1", cancel)
            .await
            .is_err()
    );
    let owned2 = ticket(&mut reader2).await.unwrap();
    assert_eq!(owned2.principal, "reader2");
    drop(reader2);
    drained(admission, 1).await;
    let replace = wire::ReplacePolicy {
        server_epoch: original.server_epoch.clone(),
        expected_version: 1,
        new_version: 2,
    };
    assert_eq!(
        action(
            &mut reader,
            "shardloom.replace-policy.v1",
            replace.encode_to_vec().into()
        )
        .await
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );
    let mut admin = client(root, "administrator", address, "localhost", true)
        .await
        .unwrap();
    action(
        &mut admin,
        "shardloom.replace-policy.v1",
        replace.encode_to_vec().into(),
    )
    .await
    .unwrap();
    assert_eq!(
        ticket(&mut admin).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    drop(admin);
    drained(admission, 1).await;
    assert!(
        action(
            &mut reader,
            "shardloom.cancel.v1",
            wire::Cancel {
                binding: Some(original)
            }
            .encode_to_vec()
            .into()
        )
        .await
        .is_err()
    );
    let fresh = ticket(&mut reader).await.unwrap();
    assert_eq!(fresh.policy_version, 2);
    action(
        &mut reader,
        "shardloom.cancel.v1",
        wire::Cancel {
            binding: Some(fresh),
        }
        .encode_to_vec()
        .into(),
    )
    .await
    .unwrap();
    let mut denied = client(root, "denied", address, "localhost", true)
        .await
        .unwrap();
    assert_eq!(
        ticket(&mut denied).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    drop((denied, reader));
    drained(admission, 0).await;
}

async fn tls_refusals(
    root: &Path,
    address: SocketAddr,
    admission: &Admission,
    authentication: &Authentication,
) -> Vec<serde_json::Value> {
    let mut outcomes = Vec::new();
    for (role, name, identity, reason) in [
        ("unknown", "localhost", true, "UnknownCA"),
        ("expired", "localhost", true, "CertificateExpired"),
        ("reader", "wrong-server.invalid", true, "NotValidForName"),
        ("reader", "localhost", false, "CertificateRequired"),
        (
            "unlisted",
            "localhost",
            true,
            "verified certificate is not in this fixture allowlist",
        ),
    ] {
        let before_lookup = authentication.lookups();
        let before_accept = admission.accepted();
        let outcome = tokio::time::timeout(IDLE * 2, async {
            let mut client = client(root, role, address, name, identity).await?;
            ticket(&mut client).await
        })
        .await
        .unwrap();
        let failure = outcome.unwrap_err();
        if role == "unlisted" {
            assert_eq!(failure.code(), Code::Unauthenticated);
        } else {
            // Tonic can report a peer TLS alert during connect or the first RPC.
            // Require the exact TLS cause, never an arbitrary transport failure.
            assert!(matches!(failure.code(), Code::Unknown | Code::Unavailable));
        }
        let details = format!("{failure:?}");
        if role == "unlisted" || name == "wrong-server.invalid" {
            assert!(
                details.contains(reason),
                "TLS case {role}/{name}/{identity}: {details}"
            );
        }
        drained(admission, 0).await;
        // A peer alert can race the next write and surface as BrokenPipe. Prove
        // accepted sockets never reached certificate lookup for TLS failures;
        // a valid but unlisted leaf must reach lookup and be denied there.
        assert!(admission.accepted() > before_accept);
        let lookups = authentication.lookups() - before_lookup;
        assert_eq!(lookups, usize::from(role == "unlisted"));
        outcomes.push(serde_json::json!({"role":role,"server_name":name,"client_identity":identity,"status":format!("{:?}",failure.code()),"observed_client_error":details,"certificate_lookups":lookups,"accepted_connections":admission.accepted()-before_accept}));
        let mut healthy = client(root, "reader", address, "localhost", true)
            .await
            .unwrap();
        assert_eq!(ticket(&mut healthy).await.unwrap().principal, "reader");
        drop(healthy);
        drained(admission, 0).await;
    }
    outcomes
}

#[test]
#[ignore = "requires fresh private TLS fixtures and the sequential local process/storage guard"]
fn real_mutual_tls_controls_recheck_identity_and_policy_per_rpc() {
    let root =
        std::env::var_os("SHARDLOOM_EXCHANGE_TLS_FIXTURES").expect("fresh TLS fixtures required");
    let root = Path::new(&root);
    let proof = runtime().block_on(async {
        let pem = Pem::load(root, "server").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let admission = Admission::new();
        let registry = Registry::new(Arc::new(Authority::load().unwrap()), Bytes::from_static(&[8; 16])).unwrap();
        let service = FlightServiceServer::new(ControlProbe { registry: registry.clone() })
            .max_decoding_message_size(MAX_RPC).max_encoding_message_size(MAX_RPC);
        let authentication = authentication(root);
        let service = InterceptedService::new(service, authenticate(authentication.clone()));
        let (shutdown, stop) = tokio::sync::oneshot::channel();
        let server = server(&pem).unwrap().add_service(service).serve_with_incoming_shutdown(
            incoming(listener, admission.clone()), async { let _ = stop.await; });
        let task = tokio::spawn(server);
        permissions(root, address, &admission).await;
        let refusals = tls_refusals(root, address, &admission, &authentication).await;
        shutdown.send(()).unwrap();
        tokio::time::timeout(IDLE, task).await.unwrap().unwrap().unwrap();
        assert_eq!(admission.snapshot(), (0, 2));
        assert_eq!(registry.snapshot().unwrap().0, 0);
        serde_json::json!({"version":1,"scope":"actual mTLS sockets, request identity and control handlers; no payload execution claim",
            "pid":std::process::id(),"listener":address.to_string(),"tls_refusals":refusals,
            "forged_header_ignored":true,"cross_principal_cancel_refused":true,"administrator_data_refused":true,
            "denied_principal_data_refused":true,"policy_rechecked_on_reused_connection":true,
            "final_live_connections":0,"peak_live_connections":2,"final_active_attempts":0})
    });
    let output =
        std::env::var_os("SHARDLOOM_EXCHANGE_TLS_REPORT").expect("new TLS report path required");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .unwrap();
    serde_json::to_writer_pretty(file, &proof).unwrap();
}
