use clap::Parser;

use waterfall::prelude::*;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// Configuration File
    #[clap(short, long, default_value = "")]
    config: String,

    /// Configuration File
    #[clap(short, long, default_value = "")]
    world: String,

    /// Enable verbose logging
    #[clap(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Parse the config
    let json = std::fs::read_to_string(&args.config)
        .expect(&format!("Unable to open {} for reading", args.config));

    // Some Deserializer.
    let world_def: WorldDefinition =
        serde_json::from_str(&json).expect("Unable to parse world definition");

    let tasks = world_def.taskset().unwrap();

    Ok(())
}
