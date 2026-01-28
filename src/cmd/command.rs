use super::Replicate;
use super::Restore;
use crate::config::Arg;
use crate::config::ArgCommand;
use crate::error::Result;

#[async_trait::async_trait]
pub trait Command {
    async fn run(&mut self) -> Result<()>;
}

pub fn command(arg: Arg) -> Result<Box<dyn Command>> {
    match &arg.cmd {
        ArgCommand::Replicate => Ok(Replicate::try_create(&arg.config)?),
        ArgCommand::ReplicaSidecar {
            force_restore,
            exec,
        } => Ok(Box::new(super::ReplicaSidecar::try_create(
            &arg.config,
            *force_restore,
            exec.clone(),
        )?)),
        ArgCommand::Restore(options) => Ok(Restore::try_create(&arg.config, options.clone())?),
    }
}
