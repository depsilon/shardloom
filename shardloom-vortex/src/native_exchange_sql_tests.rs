// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::resident_memory_source::{
    MemoryBatchSourceBuilder, exchange_bridge::PROVIDER_ALLOWANCE,
};
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt as _;

#[test]
#[ignore = "requires independent data, private TLS fixtures and the sequential process/storage guard"]
fn ordinary_sql_client_consumes_real_projection_and_enforces_ticket_ownership() {
    let report = runtime().block_on(async {
        let server = Server::start("dictionary_large").await;
        let mut client = FlightSqlServiceClient::new_from_inner(server.client("reader").await);
        let sql = server.authority.sql.clone();
        let unsupported = client.execute("SELECT unsupported FROM missing".to_owned(), None)
            .await.unwrap_err();
        assert!(unsupported.to_string().contains("SLX_UNSUPPORTED"));
        let info = client.execute(sql.clone(), None).await.unwrap();
        assert_eq!(server.session.snapshot().completed_executions, 0);
        assert_eq!(info.endpoint.len(), 1);
        let expected_rows = server.authority.case("dictionary_large").unwrap().rows;
        assert_eq!(info.total_records, i64::try_from(expected_rows).unwrap());
        assert!(info.ordered);
        let expected_schema = info.clone().try_decode_schema().unwrap();
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        let mut second = FlightSqlServiceClient::new_from_inner(server.client("reader2").await);
        let stolen = second.do_get(ticket.clone()).await.unwrap_err();
        assert!(stolen.to_string().contains("SLX_PERMISSION"), "{stolen:?}");
        drop(second);
        wait_connections(&server, 1).await;
        for role in ["denied", "administrator"] {
            let mut denied = FlightSqlServiceClient::new_from_inner(server.client(role).await);
            let error = denied.execute(sql.clone(), None).await.unwrap_err();
            assert!(error.to_string().contains("SLX_PERMISSION"));
            drop(denied);
            wait_connections(&server, 1).await;
        }
        let receiver = ResidentVortexSession::new(GRANT, 1).unwrap();
        let fixture = Fixture::load(&fixtures().0.join("fixture-manifest.json"),
            "dictionary_large", &server.authority, &receiver).unwrap();
        let decode_credit = receiver.reserve_input_scratch(PROVIDER_ALLOWANCE).unwrap();
        let cancellation = CancellationToken::default();
        let mut builder = MemoryBatchSourceBuilder::new(&receiver, cancellation.clone()).unwrap();
        let mut request = Request::new(ticket.clone());
        request.set_timeout(Duration::from_secs(60));
        let mut output = client.do_get(request).await.unwrap();
        let mut rows = 0;
        let mut batches = 0;
        while let Some(batch) = tokio::time::timeout(IDLE, output.try_next()).await.unwrap().unwrap() {
            assert_eq!(batch.schema().as_ref(), &expected_schema);
            rows += batch.num_rows();
            assert!(u64::try_from(rows).unwrap() <= expected_rows);
            batches += 1;
            assert!(batches <= 256);
            builder.push_arrow(batch).unwrap();
        }
        assert_eq!(u64::try_from(rows).unwrap(), expected_rows);
        assert!(batches > 0);
        drop(output);
        let source = builder.finish().unwrap();
        drop(decode_credit);
        server.idle().await;
        assert_eq!(server.session.snapshot().completed_executions, 1);
        let replay = client.do_get(ticket).await.unwrap_err();
        assert!(replay.to_string().contains("SLX_REPLAY"), "{replay:?}");
        drop(client);
        let sender = server.stop().await;
        assert_eq!(sender["transfers"].as_array().unwrap().len(), 1);
        assert_eq!(sender["transfers"][0]["success"], true);
        let native = &sender["transfers"][0]["send"]["native_execution"];
        assert_eq!(native["completed_executions"], 1);
        assert_eq!(native["certified"], true);
        assert_eq!(native["fallback_attempted"], false);
        let consumer = fixture.consume_sql_result(&source, &cancellation).unwrap();
        drop((source, fixture));
        assert_eq!(receiver.snapshot().memory.reserved_bytes, 0);
        json!({"version": 1, "scope": "unmodified Rust Flight SQL client and actual mTLS sockets in one process; separate-process proof remains pending",
            "sql": sql, "client": "arrow-flight-58.3.0 FlightSqlServiceClient",
            "sender": sender, "consumer": consumer, "batches": batches,
            "final_receiver_reserved_bytes": 0, "final_sender_reserved_bytes": 0,
            "unsupported_sql_refused": true, "cross_principal_ticket_refused": true,
            "completed_ticket_replay_refused": true, "unauthorized_roles_refused": 2})
    });
    let path =
        std::env::var_os("SHARDLOOM_EXCHANGE_SQL_REPORT").expect("fresh report path required");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &report).unwrap();
}

async fn wait_connections(server: &Server, expected: usize) {
    tokio::time::timeout(IDLE, async {
        while server.admission.snapshot().0 != expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
