use std::path::PathBuf;
use std::process;

use anyhow::Result;
use clap::{Parser, Subcommand};
use download_iglive::download::{download, DownloadConfig, DownloadSegments};
use download_iglive::merge::merge;

/// Download Instagram live streams, including past segments
#[derive(Parser, Debug)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Download(Download),
    Merge(Merge),
}

/// Download a live stream
#[derive(Parser, Debug)]
struct Download {
    /// URL of .mpd file
    mpd_url: String,

    /// Output directory
    #[clap(short, long)]
    output: Option<PathBuf>,

    /// Don't merge into one video file after download
    #[clap(short, long)]
    no_merge: bool,

    /// Don't download past segments
    #[clap(short, long)]
    live_only: bool,
    
    /// Number of past segments to check in parallel
    #[clap(short, long, default_value = "10")]
    parallel_candidates: usize,
}

/// Merge an already downloaded live stream into one file
#[derive(Parser, Debug)]
struct Merge {
    /// Directory to merge
    directory: PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) =  run(args).await {
        eprintln!("{e}");
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    match args.command {
        Command::Download(d) => {
            // Config
            let segments = if d.live_only {
                DownloadSegments::LIVE
            } else {
                DownloadSegments::all()
            };
            let config = DownloadConfig {
                dir: d.output,
                segments: segments,
                parallel_candidates: d.parallel_candidates,
            };

            // Download live stream
            let output_dir = download(&d.mpd_url, config).await?;

            // Merge
            if !d.no_merge {
                merge(output_dir).await?;
            }
        }
        Command::Merge(m) => merge(m.directory).await?,
    }

    Ok(())
}
