use clap::Parser;
use clap::Subcommand;

use crate::base::Generation;
use crate::error::Error;
use crate::error::Result;

#[derive(Parser, Debug)]
#[command(author="replited", version, about="Replicate sqlite to everywhere", long_about = None)]
pub struct Arg {
    #[arg(short, long, default_value = "/etc/replited.toml")]
    pub config: String,

    #[command(subcommand)]
    pub cmd: ArgCommand,
}

#[derive(Subcommand, Clone, Debug)]
pub enum ArgCommand {
    Replicate,

    /// Run replica sidecar that bootstraps (restore) then streams WAL.
    ReplicaSidecar {
        /// Force full restore even if local DB exists (will delete existing db/wal/shm first)
        #[arg(long, default_value_t = false)]
        force_restore: bool,

        /// Execute a child process (e.g. "pocketbase serve") and manage its lifecycle
        #[arg(long)]
        exec: Option<String>,
    },

    Restore(RestoreOptions),

    PurgeGeneration(PurgeGenerationOptions),
}

#[derive(Parser, Debug, Clone)]
pub struct RestoreOptions {
    // restore db path in config file
    #[arg(short, long, default_value = "")]
    pub db: String,

    // restore db output path
    #[arg(long, default_value = "")]
    pub output: String,

    // follow mode
    #[arg(short, long, default_value_t = false)]
    pub follow: bool,

    // follow interval in seconds
    #[arg(short, long, default_value_t = 1)]
    pub interval: u64,

    // restore to specific timestamp
    #[arg(short, long, default_value = "")]
    pub timestamp: String,

    // explicit archival truth-source selection for restore/follow
    #[arg(long = "truth-source", default_value = "")]
    pub truth_source: String,
}

impl RestoreOptions {
    pub fn validate(&self) -> Result<()> {
        if self.db.trim().is_empty() {
            return Err(Error::InvalidArg("arg MUST Specify db path in config"));
        }

        if self.output.trim().is_empty() {
            return Err(Error::InvalidArg("arg MUST Specify db output path"));
        }

        Ok(())
    }
}

#[derive(Parser, Debug, Clone)]
pub struct PurgeGenerationOptions {
    #[arg(short, long, default_value = "")]
    pub db: String,

    #[arg(long, default_value = "")]
    pub generation: String,
}

impl PurgeGenerationOptions {
    pub fn validate(&self) -> Result<()> {
        if self.db.trim().is_empty() {
            return Err(Error::InvalidArg("arg MUST Specify db path in config"));
        }

        if self.generation.trim().is_empty() {
            return Err(Error::InvalidArg("arg MUST Specify generation"));
        }

        Generation::try_create(&self.generation).map_err(|err| {
            Error::InvalidArg(format!("arg MUST Specify valid generation: {err}"))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Arg;
    use super::ArgCommand;
    use super::PurgeGenerationOptions;
    use super::RestoreOptions;

    #[test]
    fn purge_generation_options_reject_invalid_generation_string() {
        let err = PurgeGenerationOptions {
            db: "/tmp/test.db".to_string(),
            generation: "../../escape".to_string(),
        }
        .validate()
        .expect_err("invalid generation should fail");

        assert_eq!(err.code(), crate::error::Error::INVALID_ARG);
    }

    #[test]
    fn purge_generation_options_accept_valid_generation_string() {
        let generation = crate::base::Generation::new();

        PurgeGenerationOptions {
            db: "/tmp/test.db".to_string(),
            generation: generation.as_str().to_string(),
        }
        .validate()
        .expect("valid generation should pass");
    }

    #[test]
    fn restore_options_accept_truth_source_surface() {
        let arg = Arg::parse_from([
            "replited",
            "restore",
            "--db",
            "/tmp/test.db",
            "--output",
            "/tmp/out.db",
            "--truth-source",
            "s3-primary",
        ]);

        match arg.cmd {
            ArgCommand::Restore(options) => {
                assert_eq!(options.truth_source, "s3-primary");
            }
            other => panic!("expected restore command, got {other:?}"),
        }
    }

    #[test]
    fn restore_options_validate_without_truth_source() {
        RestoreOptions {
            db: "/tmp/test.db".to_string(),
            output: "/tmp/out.db".to_string(),
            follow: false,
            interval: 1,
            timestamp: String::new(),
            truth_source: String::new(),
        }
        .validate()
        .expect("truth source remains optional in PR 1");
    }

    #[test]
    fn restore_options_reject_whitespace_only_paths() {
        let err = RestoreOptions {
            db: "   ".to_string(),
            output: "   ".to_string(),
            follow: false,
            interval: 1,
            timestamp: String::new(),
            truth_source: String::new(),
        }
        .validate()
        .expect_err("whitespace-only paths should fail");

        assert_eq!(err.code(), crate::error::Error::INVALID_ARG);
    }

    #[test]
    fn purge_generation_options_reject_whitespace_only_values() {
        let err = PurgeGenerationOptions {
            db: "   ".to_string(),
            generation: "   ".to_string(),
        }
        .validate()
        .expect_err("whitespace-only values should fail");

        assert_eq!(err.code(), crate::error::Error::INVALID_ARG);
    }
}
