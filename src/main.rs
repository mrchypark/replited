use clap::Parser;
use replited::cmd::command;
use replited::config::Arg;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arg = Arg::parse();
    println!("arg: {:?}\n", arg);

    let mut cmd = command(arg)?;

    cmd.run().await?;

    Ok(())
}
