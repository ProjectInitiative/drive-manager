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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        std::env::set_var("DRYRUN", "true");
        std::env::set_var("CONFIG", "/path/to/config");
        std::env::set_var("THREADS", "8");
        let args = Args::parse();
        assert!(args.dryrun);
        assert_eq!(args.config, "/path/to/config");
        assert_eq!(args.threads, 8);
    }
}
