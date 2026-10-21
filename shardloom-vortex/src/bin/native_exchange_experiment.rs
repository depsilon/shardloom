// SPDX-License-Identifier: Apache-2.0

#[cfg(unix)]
fn main() -> std::process::ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let result = match args.as_slice() {
        [role]
            if matches!(
                role.as_str(),
                "sender" | "receiver" | "receiver-hold" | "receiver-stale"
            ) =>
        {
            shardloom_vortex::run_native_exchange_experiment_peer(role)
        }
        _ => Err(
            "SLX_CONFIG: expected a finite peer role; bounded configuration arrives on stdin"
                .to_owned(),
        ),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(unix))]
fn main() -> std::process::ExitCode {
    eprintln!("SLX_CONFIG: the finite exchange experiment requires Unix native sources");
    std::process::ExitCode::FAILURE
}
