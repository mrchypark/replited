use std::fs;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use replited::experimental::ltx_compare::{
    decode_ltx_snapshot_to_db, decode_manifest_snapshot_to_db, encode_ltx_snapshot_from_db,
    encode_manifest_snapshot_from_db,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum SnapshotFormat {
    Manifest,
    Ltx,
}

#[derive(Debug, Parser)]
#[command(name = "ltx_probe")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    EncodeSnapshot {
        #[arg(long, value_enum)]
        format: SnapshotFormat,
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    DecodeSnapshot {
        #[arg(long, value_enum)]
        format: SnapshotFormat,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::EncodeSnapshot { format, db, out } => {
            let bytes = match format {
                SnapshotFormat::Manifest => encode_manifest_snapshot_from_db(&db)?,
                SnapshotFormat::Ltx => encode_ltx_snapshot_from_db(&db)?,
            };
            fs::write(out, bytes)?;
        }
        Command::DecodeSnapshot {
            format,
            input,
            output,
        } => {
            let bytes = fs::read(input)?;
            match format {
                SnapshotFormat::Manifest => decode_manifest_snapshot_to_db(&bytes, &output)?,
                SnapshotFormat::Ltx => decode_ltx_snapshot_to_db(&bytes, &output)?,
            }
        }
    }
    Ok(())
}
