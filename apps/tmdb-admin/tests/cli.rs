use std::process::Command;

#[test]
fn help_lists_operational_commands() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_tmdb-admin"))
        .arg("--help")
        .output()?;
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout)?;
    for command in ["doctor", "submit-noop", "job-status", "migrate"] {
        assert!(help.contains(command), "missing command {command} in help");
    }
    Ok(())
}

#[test]
fn doctor_requires_json_output_flag() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_tmdb-admin"))
        .arg("doctor")
        .env("TMDB_ENVIRONMENT", "development")
        .env("POSTGRES_DB", "example_catalog")
        .env("POSTGRES_USER", "example_owner")
        .env("POSTGRES_PASSWORD", "test-only-not-read")
        .output()?;
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("doctor requires --json"));
    Ok(())
}

#[test]
fn legacy_database_cli_options_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_tmdb-admin"))
        .args(["--host", "127.0.0.1", "doctor"])
        .env("TMDB_ENVIRONMENT", "development")
        .env("POSTGRES_DB", "example_catalog")
        .env("POSTGRES_USER", "example_owner")
        .env("POSTGRES_PASSWORD", "test-only-not-read")
        .output()?;
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(stderr.contains("unexpected argument '--host'"));
    Ok(())
}
