//! `kurator`: run a replica, or make a node key. See [`kurator_node::args`].

use std::io::Write as _;
use std::process::ExitCode;

use kurator_node::args::{self, Invocation};

#[tokio::main]
async fn main() -> ExitCode {
    let outcome = match args::parse(std::env::args().skip(1)) {
        Ok(Invocation::Key(dir)) => kurator_node::key(&dir)
            .map(|key| Some(key.to_string()))
            .map_err(|error| error.to_string()),
        Ok(Invocation::Run(settings)) => kurator_node::run(settings)
            .await
            .map(|()| None)
            .map_err(|error| error.to_string()),
        Ok(_) => Err("unsupported command".to_owned()),
        Err(usage) => Err(usage.to_string()),
    };
    report(outcome).unwrap_or(ExitCode::FAILURE)
}

/// Shows the outcome: what to show on stdout, if anything, or the error on
/// stderr.
fn report(outcome: Result<Option<String>, String>) -> std::io::Result<ExitCode> {
    match outcome {
        Ok(None) => Ok(ExitCode::SUCCESS),
        Ok(Some(shown)) => {
            writeln!(std::io::stdout(), "{shown}")?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            writeln!(std::io::stderr(), "kurator: {error}")?;
            Ok(ExitCode::FAILURE)
        }
    }
}
