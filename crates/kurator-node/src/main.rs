//! `kurator`: run a replica, or make a node key. See [`kurator_node::args`].

use std::io::Write as _;
use std::process::ExitCode;

use kurator_node::args::{self, Invocation};

#[tokio::main]
async fn main() -> ExitCode {
    let outcome = match args::parse(std::env::args().skip(1)) {
        Ok(Invocation::Key(dir)) => kurator_node::key(&dir)
            .map(|key| key.to_string())
            .map_err(|error| error.to_string()),
        Ok(Invocation::Run(settings)) => kurator_node::run(settings)
            .await
            .map(|()| String::new())
            .map_err(|error| error.to_string()),
        Ok(_) => Err("unsupported command".to_owned()),
        Err(usage) => Err(usage.to_string()),
    };
    report(outcome).unwrap_or(ExitCode::FAILURE)
}

/// Shows the outcome: what to show on stdout, or the error on stderr.
fn report(outcome: Result<String, String>) -> std::io::Result<ExitCode> {
    match outcome {
        Ok(shown) if shown.is_empty() => Ok(ExitCode::SUCCESS),
        Ok(shown) => {
            writeln!(std::io::stdout(), "{shown}")?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            writeln!(std::io::stderr(), "kurator: {error}")?;
            Ok(ExitCode::FAILURE)
        }
    }
}
