pub struct Args {
    pub dryrun: bool,
    pub config: String,
    pub threads: usize,
}

impl Args {
    pub fn parse() -> Self {
        let dryrun = std::env::args().any(|arg| arg == "--dryrun");
        let config = std::env::args().find(|arg| arg.starts_with("-c") || arg.starts_with("--config")).unwrap_or(CONFIG_FILE_PATH.to_string());
        let threads = std::env::args().find(|arg| arg.starts_with("-t") || arg.starts_with("--threads")).unwrap_or(IO_THREADS.to_string()).parse().unwrap();
        Self { dryrun, config, threads }
    }
}
