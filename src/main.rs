use ferrited::config::Config;

fn main() {
    let config = Config::from_args(std::env::args_os()).unwrap_or_else(|error| error.exit());
    if let Err(error) = run(config) {
        eprintln!("ferrited: {error}");
        std::process::exit(1);
    }
}

fn run(config: Config) -> std::io::Result<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // Client connections have event loop threads of their own (`Config::client_threads`).
    builder.worker_threads(config.runtime_threads());
    builder.build()?.block_on(ferrited::server::start(config))
}
