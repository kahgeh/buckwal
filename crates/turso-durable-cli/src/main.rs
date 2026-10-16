use std::process::ExitCode;

fn main() -> ExitCode {
    eprintln!("usage: turso-durable <command> [options]");
    eprintln!("(no commands are implemented yet)");
    ExitCode::from(2)
}
