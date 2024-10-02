use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use serde_json::Value;
use log::{info, error};
use simple_logger::SimpleLogger;

const CONFIG_FILE_PATH: &str = "/etc/drive-manager/config.json";
const IO_THREADS: usize = 4;
const TIERING_CHECK_INTERVAL: u64 = 7200; // 2 hours in seconds

struct DriveManager {
    args: Args,
    config: Value,
    new_drive_mounted: bool,
    tiering_manager: TieringManager,
}

impl DriveManager {
    const MOUNT_PATH: &'static str = "/mnt/physical";
    const MERGERFS_MOUNT_PATH: &'static str = "/mnt/merged";
    const LSBLK_DISCOVER_CMD: [&'static str; 3] = ["lsblk", "--all", "-po"];

    fn new(args: Args) -> Self {
        let config = Self::read_config(&args);
        let new_drive_mounted = false;
        let tiering_manager = TieringManager::new(Arc::new(Mutex::new(self)));
        Self { args, config, new_drive_mounted, tiering_manager }
    }

    fn run_command(&self, cmd: &[&str]) -> Result<(), std::io::Error> {
        if self.args.dryrun {
            info!("DRYRUN: {}", cmd.join(" "));
            return Ok(());
        }
        info!("{}", cmd.join(" "));
        Command::new(cmd[0]).args(&cmd[1..]).status()?;
        Ok(())
    }

    fn get_atime(directory: &str) -> HashMap<String, SystemTime> {
        let mut atime_dict = HashMap::new();
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                let metadata = fs::metadata(&path).unwrap();
                atime_dict.insert(path.to_str().unwrap().to_string(), metadata.accessed().unwrap());
            }
        }
        atime_dict
    }

    fn rsync(&self, src: &str, dest: &str) -> bool {
        let rsync_command = ["rsync", "-axqHAXWES", "--preallocate", "--remove-source-files", src, dest];
        if self.args.dryrun {
            info!("[DRY RUN] Would run rsync command: {}", rsync_command.join(" "));
            return true;
        }
        info!("Running rsync command: {}", rsync_command.join(" "));
        match Command::new(rsync_command[0]).args(&rsync_command[1..]).status() {
            Ok(status) => status.success(),
            Err(e) => {
                error!("Rsync command failed: {}", e);
                false
            }
        }
    }

    fn sort_block_device(&self, block_device: &Value) -> i32 {
        let class_order = HashMap::from([("nvme", 0), ("ssd", 1), ("hdd", 2)]);
        let block_class = block_device["block_class"].as_str().unwrap();
        *class_order.get(block_class).unwrap()
    }

    fn setup_mergerfs(&self, active_block_devices: Vec<Value>) {
        let mergerfs_opts = vec![
            "allow_other",
            "nonempty",
            "lazy-umount-mountpoint=true",
            "moveonenospc=true",
            "cache.files=auto-full",
            "parallel-direct-writes=true",
            "cache.writeback=true",
            "cache.statfs=true",
            "cache.symlinks=true",
            "cache.readdir=true",
            "posix_acl=false",
            "async_read=false",
            "dropcacheonclose=true",
        ];

        let mut tier_devices = HashMap::new();
        tier_devices.insert("hot", active_block_devices.clone());
        tier_devices.insert("warm", active_block_devices.iter().filter(|device| device["block_class"] != "nvme").cloned().collect());
        tier_devices.insert("cold", active_block_devices.iter().filter(|device| device["block_class"] == "hdd").cloned().collect());

        for (tier, devices) in &mut tier_devices {
            devices.sort_by_key(|device| self.sort_block_device(device));
        }

        let tier_globs: HashMap<&str, String> = tier_devices.iter().map(|(tier, devices)| {
            let glob = devices.iter().map(|device| device["children"][0]["mountpoint"].as_str().unwrap()).collect::<Vec<&str>>().join(":");
            (*tier, glob)
        }).collect();

        for (tier, glob) in tier_globs {
            let mount_point = format!("{}/{}", Self::MERGERFS_MOUNT_PATH, tier);
            fs::create_dir_all(&mount_point).unwrap();
            let mergerfs_cmd = if tier == "cold" {
                ["mergerfs", "-o", &mergerfs_opts.join(",") + ",category.create=mfs", &glob, &mount_point]
            } else {
                ["mergerfs", "-o", &mergerfs_opts.join(",") + ",category.create=ff", &glob, &mount_point]
            };
            self.run_command(&mergerfs_cmd).unwrap();
        }
        self.tiering_manager.start_background_process();
    }

    fn mount_drive(&mut self, block_device: &Value) -> Value {
        let mount_point = format!("{}/{}/{}", Self::MOUNT_PATH, block_device["block_class"].as_str().unwrap(), block_device["serial"].as_str().unwrap());
        fs::create_dir_all(&mount_point).unwrap();
        let part_path = block_device["children"][0]["path"].as_str().unwrap();
        let part_mount_point = block_device["children"][0]["mountpoint"].as_str().unwrap();
        if part_mount_point != mount_point {
            self.run_command(&["mount", part_path, &mount_point]).unwrap();
            self.new_drive_mounted = true;
        }
        self.update_block_device(block_device)
    }

    fn format_drive(&mut self, block_device: &Value) -> Value {
        let filesystem = self.config.get("filesystem").unwrap().as_str().unwrap();
        if block_device.get("children").is_some() {
            for partition in block_device["children"].as_array().unwrap() {
                self.run_command(&["umount", "-l", partition["path"].as_str().unwrap()]).unwrap();
            }
        }
        self.run_command(&["wipefs", "--all", "--force", block_device["path"].as_str().unwrap()]).unwrap();
        self.run_command(&["parted", "-a", "optimal", block_device["path"].as_str().unwrap(), "mklabel", "gpt", "mkpart", "primary", filesystem, "0%", "100%"]).unwrap();
        let updated_device = self.update_block_device(block_device);
        let parts = &updated_device["children"].as_array().unwrap()[0];
        self.run_command(&["yes", "|", "mkfs", "-t", filesystem, parts["path"].as_str().unwrap()]).unwrap();
        self.mount_drive(&updated_device)
    }

    fn read_config(args: &Args) -> Value {
        let config_file = fs::read_to_string(args.config.clone()).unwrap();
        serde_json::from_str(&config_file).unwrap()
    }

    fn update_block_device(&self, block_device: &Value) -> Value {
        let output = Command::new("lsblk").args(Self::LSBLK_DISCOVER_CMD).arg(block_device["path"].as_str().unwrap()).output().unwrap();
        let block_device: Value = serde_json::from_str(&String::from_utf8_lossy(&output.stdout)).unwrap();
        self.classify_block_class(&mut block_device);
        block_device
    }

    fn classify_block_class(&self, block_device: &mut Value) {
        let rota = block_device["rota"].as_bool().unwrap();
        let tran = block_device["tran"].as_str().unwrap();
        let (block_class, tier) = if !rota {
            if tran == "nvme" {
                ("nvme", "hot")
            } else {
                ("ssd", "warm")
            }
        } else {
            ("hdd", "cold")
        };
        block_device["tier"] = serde_json::Value::String(tier.to_string());
        block_device["block_class"] = serde_json::Value::String(block_class.to_string());
    }

    fn get_block_devices(&self) -> Vec<Value> {
        let output = Command::new("lsblk").args(&["-dno", "path,type", "--json"]).output().unwrap();
        let drives_dict: Value = serde_json::from_str(&String::from_utf8_lossy(&output.stdout)).unwrap();
        let mut block_devices = Vec::new();
        for block_device in drives_dict["blockdevices"].as_array().unwrap() {
            if block_device["type"].as_str().unwrap() == "disk" {
                block_devices.push(self.update_block_device(block_device));
            }
        }
        block_devices
    }
}

struct TieringManager {
    drive_manager: Arc<Mutex<DriveManager>>,
    db_path: String,
    db: shelve::Shelf,
    move_queue: mpsc::Sender<FileMoveInfo>,
    retry_queue: mpsc::Sender<FileMoveInfo>,
    executor: threadpool::ThreadPool,
}

impl TieringManager {
    fn new(drive_manager: Arc<Mutex<DriveManager>>) -> Self {
        let db_path = "/etc/drive-manager/file_metadata.db".to_string();
        let db = shelve::open(&db_path).unwrap();
        let (move_tx, move_rx) = mpsc::channel();
        let (retry_tx, retry_rx) = mpsc::channel();
        let executor = threadpool::ThreadPool::new(IO_THREADS);
        Self { drive_manager, db_path, db, move_queue: move_tx, retry_queue: retry_tx, executor }
    }

    fn start_background_process(&self) {
        thread::spawn(move || self.tiering_check_loop());
        thread::spawn(move || self.file_mover_loop(move_rx));
        thread::spawn(move || self.retry_loop(retry_rx));
        thread::spawn(move || self.maintenance_loop());
    }

    fn tiering_check_loop(&self) {
        loop {
            self.perform_tiering_check();
            thread::sleep(Duration::from_secs(TIERING_CHECK_INTERVAL));
        }
    }

    fn file_mover_loop(&self, rx: Receiver<FileMoveInfo>) {
        for file_info in rx {
            self.executor.execute(move || self.move_file(file_info));
        }
    }

    fn retry_loop(&self, rx: Receiver<FileMoveInfo>) {
        for file_info in rx {
            if file_info.retries < 3 {
                let mut new_info = file_info.clone();
                new_info.retries += 1;
                self.move_queue.send(new_info).unwrap();
            } else {
                error!("Failed to move file after 3 retries: {}", file_info.src);
            }
        }
    }

    fn perform_tiering_check(&self) {
        info!("Starting tiering check");
        self.update_file_metadata();
        self.check_tier_capacities();
        self.move_files_based_on_rules();
        info!("Tiering check completed");
    }

    fn update_file_metadata(&self) {
        for tier in ["hot", "warm", "cold"].iter() {
            let tier_path = format!("{}/{}", self.drive_manager.lock().unwrap().MERGERFS_MOUNT_PATH, tier);
            for entry in fs::read_dir(&tier_path).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_file() {
                    let relative_path = path.strip_prefix(self.drive_manager.lock().unwrap().MERGERFS_MOUNT_PATH).unwrap().to_str().unwrap().to_string();
                    let metadata = fs::metadata(&path).unwrap();
                    let atime = metadata.accessed().unwrap();
                    let size = metadata.len();
                    if let Some(mut file_info) = self.db.get(&relative_path) {
                        file_info.last_access_time = atime;
                        file_info.access_count += 1;
                        file_info.file_size = size;
                        file_info.tier = tier.to_string();
                        self.db.insert(relative_path.clone(), file_info);
                    } else {
                        self.db.insert(relative_path.clone(), FileMetadata {
                            last_access_time: atime,
                            access_count: 1,
                            file_size: size,
                            tier: tier.to_string(),
                        });
                    }
                }
            }
        }
        self.db.sync().unwrap();
    }

    fn check_tier_capacities(&self) {
        for tier in ["hot", "warm", "cold"].iter() {
            let tier_path = format!("{}/{}", self.drive_manager.lock().unwrap().MERGERFS_MOUNT_PATH, tier);
            let metadata = fs::metadata(&tier_path).unwrap();
            let total = metadata.len();
            let used = total - metadata.available();
            let usage_percent = (used as f64 / total as f64) * 100.0;
            if usage_percent > self.drive_manager.lock().unwrap().config.get("tier_capacity_threshold").unwrap().as_f64().unwrap() {
                self.move_files_down(tier);
            }
        }
    }

    fn move_files_down(&self, source_tier: &str) {
        let target_tier = if source_tier == "hot" { "warm" } else { "cold" };
        let files_to_move: Vec<_> = self.db.iter().filter(|(_, file_info)| file_info.tier == source_tier).collect();
        for (file_path, _) in files_to_move.iter().take(10) {
            self.queue_file_move(file_path.clone(), source_tier.to_string(), target_tier.to_string());
        }
    }

    fn move_files_based_on_rules(&self) {
        let access_time_threshold = SystemTime::now() - Duration::from_secs(self.drive_manager.lock().unwrap().config.get("access_time_threshold").unwrap().as_u64().unwrap());
        let access_count_threshold = self.drive_manager.lock().unwrap().config.get("access_count_threshold").unwrap().as_u64().unwrap();
        for (file_path, file_info) in self.db.iter() {
            if file_info.access_count >= access_count_threshold && file_info.last_access_time > access_time_threshold && file_info.tier != "hot" {
                self.queue_file_move(file_path.clone(), file_info.tier.clone(), "hot".to_string());
            }
        }
    }

    fn queue_file_move(&self, file_path: String, source_tier: String, target_tier: String) {
        self.move_queue.send(FileMoveInfo {
            src: file_path,
            source_tier,
            target_tier,
            retries: 0,
        }).unwrap();
    }

    fn move_file(&self, file_info: FileMoveInfo) {
        let src = file_info.src;
        let source_tier = file_info.source_tier;
        let target_tier = file_info.target_tier;
        let source_path = format!("{}/{}", self.drive_manager.lock().unwrap().MERGERFS_MOUNT_PATH, source_tier);
        let target_path = format!("{}/{}", self.drive_manager.lock().unwrap().MERGERFS_MOUNT_PATH, target_tier);
        let relative_path = Path::new(&src).strip_prefix(&source_path).unwrap().to_str().unwrap().to_string();
        let dest = format!("{}/{}", target_path, relative_path);
        fs::create_dir_all(Path::new(&dest).parent().unwrap()).unwrap();
        if self.drive_manager.lock().unwrap().rsync(&src, &dest) {
            info!("Moved file from {} to {}", src, dest);
            if let Some(mut file_info) = self.db.get(&relative_path) {
                file_info.tier = target_tier;
                file_info.last_tier_move = SystemTime::now();
                self.db.insert(relative_path.clone(), file_info);
            }
            self.db.sync().unwrap();
        } else {
            error!("Failed to move file {}. Queueing for retry.", src);
            self.retry_queue.send(file_info).unwrap();
        }
    }

    fn validate_and_update_database(&self) {
        info!("Starting database validation and update");
        for tier in ["hot", "warm", "cold"].iter() {
            let tier_path = format!("{}/{}", self.drive_manager.lock().unwrap().MERGERFS_MOUNT_PATH, tier);
            for entry in fs::read_dir(&tier_path).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_file() {
                    let relative_path = path.strip_prefix(self.drive_manager.lock().unwrap().MERGERFS_MOUNT_PATH).unwrap().to_str().unwrap().to_string();
                    if let Some(mut file_info) = self.db.get(&relative_path) {
                        if file_info.tier != *tier {
                            info!("Updating tier for {} from {} to {}", relative_path, file_info.tier, tier);
                            file_info.tier = tier.to_string();
                            self.db.insert(relative_path.clone(), file_info);
                        }
                    } else {
                        info!("Adding new file to database: {}", relative_path);
                        self.db.insert(relative_path.clone(), FileMetadata {
                            tier: tier.to_string(),
                            last_access_time: fs::metadata(&path).unwrap().accessed().unwrap(),
                            access_count: 1,
                            file_size: fs::metadata(&path).unwrap().len(),
                        });
                    }
                }
            }
        }
        let mut to_remove = Vec::new();
        for (relative_path, _) in self.db.iter() {
            let full_path = format!("{}/{}", self.drive_manager.lock().unwrap().MERGERFS_MOUNT_PATH, relative_path);
            if !Path::new(&full_path).exists() {
                to_remove.push(relative_path.clone());
                info!("Removing non-existent file from database: {}", relative_path);
            }
        }
        for relative_path in to_remove {
            self.db.remove(&relative_path);
        }
        self.db.sync().unwrap();
        info!("Database validation and update completed");
    }

    fn maintenance_loop(&self) {
        loop {
            self.validate_and_update_database();
            thread::sleep(Duration::from_secs(86400));
        }
    }
}

#[derive(Clone)]
struct FileMoveInfo {
    src: String,
    source_tier: String,
    target_tier: String,
    retries: u32,
}

#[derive(Clone)]
struct FileMetadata {
    last_access_time: SystemTime,
    access_count: u64,
    file_size: u64,
    tier: String,
}

struct Args {
    dryrun: bool,
    config: String,
    threads: usize,
}

fn main() {
    SimpleLogger::new().init().unwrap();
    let args = Args {
        dryrun: std::env::args().any(|arg| arg == "--dryrun"),
        config: std::env::args().find(|arg| arg.starts_with("-c") || arg.starts_with("--config")).unwrap_or(CONFIG_FILE_PATH.to_string()),
        threads: std::env::args().find(|arg| arg.starts_with("-t") || arg.starts_with("--threads")).unwrap_or(IO_THREADS.to_string()).parse().unwrap(),
    };
    let drive_manager = DriveManager::new(args);
    let config = drive_manager.config.clone();
    let exclude_drives = config.get("exclude_drives").unwrap().as_array().unwrap();
    let filesystem = config.get("filesystem").unwrap().as_str().unwrap();
    info!("Excluding drives: {:?}", exclude_drives);
    let block_devices = drive_manager.get_block_devices();
    let mut active_drives = Vec::new();
    for block_device in block_devices {
        let serial = block_device["serial"].as_str().unwrap();
        let path = block_device["path"].as_str().unwrap();
        let block_class = block_device["block_class"].as_str().unwrap();
        let partitions = block_device.get("children");
        if exclude_drives.contains(&serde_json::Value::String(serial.to_string())) {
            info!("{} {} to be excluded", path, serial);
        } else if partitions.is_some() && partitions.unwrap().as_array().unwrap().len() == 1 && partitions.unwrap()[0]["fstype"].as_str().unwrap() == filesystem {
            info!("{} {} to be mounted as {}", path, serial, block_class);
            active_drives.push(drive_manager.mount_drive(&block_device));
        } else {
            info!("{} {} to be formatted as {}", path, serial, block_class);
            active_drives.push(drive_manager.format_drive(&block_device));
        }
    }
    drive_manager.setup_mergerfs(active_drives);
    drive_manager.tiering_manager.start_background_process();
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
