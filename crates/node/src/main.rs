//! `oleaph`: run a replica, make a node key, or run a client. See [`oleaph_node::args`].

use std::io::Write as _;
use std::process::ExitCode;

use oleaph_node::args::{self, Invocation};

#[tokio::main]
async fn main() -> ExitCode {
    let outcome = match args::parse(std::env::args().skip(1)) {
        Ok(Invocation::Key(dir, purpose)) => oleaph_node::key(&dir, purpose)
            .map(|key| Some(key.to_string()))
            .map_err(|error| error.to_string()),
        Ok(Invocation::Run(settings)) => oleaph_node::run(settings)
            .await
            .map(|()| None)
            .map_err(|error| error.to_string()),
        Ok(Invocation::Client(session)) => oleaph_node::session::run(&session)
            .await
            .map(Some)
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
            writeln!(std::io::stderr(), "oleaph: {error}")?;
            Ok(ExitCode::FAILURE)
        }
    }
}
