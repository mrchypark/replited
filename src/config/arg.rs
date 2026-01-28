use clap::Parser;
use clap::Subcommand;

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
}

impl RestoreOptions {
    pub fn validate(&self) -> Result<()> {
        if self.db.is_empty() {
            return Err(Error::InvalidArg("arg MUST Specify db path in config"));
        }

        if self.output.is_empty() {
            return Err(Error::InvalidArg("arg MUST Specify db output path"));
        }

        Ok(())
    }
}
