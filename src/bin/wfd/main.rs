use actix_cors::Cors;
use actix_web::{error, middleware::Logger, web, App, HttpResponse, HttpServer, Responder};
use clap::Parser;
use serde::{Deserialize, Serialize};

use tokio::sync::{mpsc, oneshot};
use waterfall::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerConfig {
    pub ip: String,
    pub port: u32,
}

impl ServerConfig {
    fn listen_spec(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            ip: String::from("127.0.0.1"),
            port: 2503,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", deny_unknown_fields, tag = "type")]
enum StorageConfig {
    Redis { url: String, prefix: String },
}

impl StorageConfig {
    fn start(
        &self,
    ) -> (
        mpsc::UnboundedSender<StorageMessage>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        match self {
            StorageConfig::Redis { url, prefix } => (
                tx,
                waterfall::storage::redis::start(rx, url.clone(), prefix.clone()),
            ),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", deny_unknown_fields, tag = "type")]
enum ExecutorConfig {
    Local {
        workers: usize,
    },
    Agent {
        targets: Vec<agent_executor::AgentTarget>,
    },
}

impl ExecutorConfig {
    fn start(
        &self,
    ) -> (
        mpsc::UnboundedSender<ExecutorMessage>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        match self {
            ExecutorConfig::Local { workers } => (tx, local_executor::start(*workers, rx)),
            ExecutorConfig::Agent { targets } => (tx, agent_executor::start(targets.clone(), rx)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct Config {
    storage: StorageConfig,
    executor: ExecutorConfig,
    server: ServerConfig,
}

#[derive(Serialize)]
struct SimpleError {
    error: String,
}

async fn get_state(state: web::Data<AppState>) -> impl Responder {
    // Build the current state
    // let (response, rx) = oneshot::channel();

    /*
    state
        .config
        .tracker
        .send(TrackerMessage::GetRun { run_id, response })
        .unwrap();

    match rx.await.unwrap() {
        Ok(run) => HttpResponse::Ok().json(run),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
    */
    HttpResponse::Ok().body("")
}

/*
async fn stop_run(path: web::Path<RunID>, state: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    state
        .config
        .runner
        .send(RunnerMessage::StopRun { run_id, response })
        .unwrap();

    rx.await.unwrap();
    HttpResponse::Ok()
}
*/

async fn ready() -> impl Responder {
    HttpResponse::Ok()
}

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

    /// Force a full re-check
    #[clap(short, long)]
    force_recheck: bool,
}

#[derive(Clone)]
struct AppState {
    exe_tx: mpsc::UnboundedSender<ExecutorMessage>,
    storage_tx: mpsc::UnboundedSender<StorageMessage>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    // Parse the config
    let world_json = std::fs::read_to_string(&args.world)
        .expect(&format!("Unable to open {} for reading", args.config));
    let world_def: WorldDefinition =
        serde_json::from_str(&world_json).expect("Unable to parse world definition");

    // Parse the config
    let config_json = std::fs::read_to_string(&args.config)
        .expect(&format!("Unable to open {} for reading", args.config));
    let config: Config =
        serde_json::from_str(&config_json).expect("Unable to parse config definition");

    // Start the workers
    let (exe_tx, exe_handle) = config.executor.start();
    let (storage_tx, storage_handle) = config.storage.start();

    let data = web::Data::new(AppState {
        exe_tx: exe_tx.clone(),
        storage_tx: storage_tx.clone(),
    });

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let res = HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_header()
            .allow_any_method()
            .allow_any_origin()
            .send_wildcard();

        let json_config = web::JsonConfig::default()
            .limit(1048576)
            .error_handler(|err, _req| {
                use actix_web::error::JsonPayloadError;
                let payload = match &err {
                    JsonPayloadError::OverflowKnownLength { length, limit } => SimpleError {
                        error: format!("Payload too big ({} > {})", length, limit),
                    },
                    JsonPayloadError::Overflow { limit } => SimpleError {
                        error: format!("Payload too big (> {})", limit),
                    },
                    JsonPayloadError::ContentType => SimpleError {
                        error: "Unsupported Content-Type".to_owned(),
                    },
                    JsonPayloadError::Deserialize(e) => SimpleError {
                        error: format!("Parsing error: {}", e),
                    },
                    JsonPayloadError::Serialize(e) => SimpleError {
                        error: format!("JSON Generation error: {}", e),
                    },
                    JsonPayloadError::Payload(payload) => SimpleError {
                        error: format!("Payload error: {}", payload),
                    },
                    _ => SimpleError {
                        error: "Unknown error".to_owned(),
                    },
                };

                error::InternalError::from_response(err, HttpResponse::Conflict().json(payload))
                    .into()
            });

        App::new()
            .wrap(cors)
            .app_data(data.clone())
            .wrap(Logger::new(
                r#"%a "%r" %s %b "%{Referer}i" "%{User-Agent}i" %T"#,
            ))
            .app_data(json_config)
            .route("/ready", web::get().to(ready))
            .service(web::scope("/api/v1/").route("", web::get().to(get_state)))
    })
    .bind(config.server.listen_spec())?
    .run()
    .await;

    // Shutdown the runner
    exe_tx.send(ExecutorMessage::Stop {}).unwrap();
    exe_handle.await.unwrap();
    storage_tx.send(StorageMessage::Stop {}).unwrap();
    storage_handle.await.unwrap();

    res
}
