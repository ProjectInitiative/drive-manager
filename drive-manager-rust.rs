use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use rusqlite::{Connection, Result as SqliteResult, params};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crossbeam_channel::{bounded, Sender, Receiver};
use log::{info, warn, error};
use clap::{App, Arg};
use anyhow::{Context, Result};
use walkdir::WalkDir;

const CONFIG_FILE_PATH: &str = "/etc/drive-manager/config.json";
const IO_THREADS: usize = 4;
const TIERING_CHECK_INTERVAL: u64 = 7200; // 2 hours in seconds

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    filesystem: String,
    exclude_drives: Vec<String>,
    tier_capacity_threshold: f64,
    access_time_threshold: u64,
    access_count_threshold: u32,
}

#[derive(Debug, Clone)]
struct BlockDevice {
    path: String,
    serial: String,
    block_class: String,
    tier: String,
    children: Vec<Partition>,
}

#[derive(Debug, Clone)]
struct Partition {
    path: String,
    fstype: String,
    mountpoint: String,
}

struct DriveManager {
    config: Config,
    new_drive_mounted: bool,
    dryrun: bool,
}

struct TieringManager {
    drive_manager: Arc<Mutex<DriveManager>>,
    db_path: String,
}

impl DriveManager {
    fn new(config_path: &str, dryrun: bool) -> Result<Self> {
        let config = Self::read_config(config_path)?;
        Ok(DriveManager {
            config,
            new_drive_mounted: false,
            dryrun,
        })
    }

    fn read_config(path: &str) -> Result<Config> {
        let config_str = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;
        let config: Config = serde_json::from_str(&config_str)
            .with_context(|| format!("Failed to parse config JSON: {}", path))?;
        Ok(config)
    }

    fn run_command(&self, cmd: &str, args: &[&str]) -> Result<String> {
        if self.dryrun {
            info!("DRYRUN: Would run command: {} {}", cmd, args.join(" "));
            Ok(String::new())
        } else {
            let output = Command::new(cmd)
                .args(args)
                .output()
                .with_context(|| format!("Failed to execute command: {} {}", cmd, args.join(" ")))?;
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        }
    }

    fn mount_drive(&mut self, block_device: &BlockDevice) -> Result<BlockDevice> {
        let mount_point = format!("/mnt/physical/{}/{}", block_device.block_class, block_device.serial);
        fs::create_dir_all(&mount_point)
            .with_context(|| format!("Failed to create mount point: {}", mount_point))?;
        
        let part_path = &block_device.children[0].path;
        if block_device.children[0].mountpoint != mount_point {
            self.run_command("mount", &[part_path, &mount_point])?;
            self.new_drive_mounted = true;
        }
        
        self.update_block_device(block_device)
    }

    fn format_drive(&self, block_device: &BlockDevice) -> Result<BlockDevice> {
        // Umount all partitions
        for partition in &block_device.children {
            self.run_command("umount", &["-l", &partition.path])?;
        }

        // Wipe other filesystems
        self.run_command("wipefs", &["--all", "--force", &block_device.path])?;

        // Create new blank partition
        self.run_command("parted", &["-a", "optimal", &block_device.path, "mklabel", "gpt", "mkpart", "primary", &self.config.filesystem, "0%", "100%"])?;

        // Make specified filesystem
        self.run_command("mkfs", &["-t", &self.config.filesystem.to_lowercase(), &block_device.children[0].path])?;

        // Mount partition
        self.mount_drive(&self.update_block_device(block_device)?)
    }

    fn update_block_device(&self, block_device: &BlockDevice) -> Result<BlockDevice> {
        let output = self.run_command("lsblk", &["--json", "-po", "NAME,PATH,FSTYPE,MOUNTPOINT,SERIAL,ROTA,TRAN", &block_device.path])?;
        let v: Value = serde_json::from_str(&output)?;
        
        if let Some(device) = v["blockdevices"].as_array()?.get(0) {
            let serial = device["serial"].as_str().unwrap_or("").to_string();
            let rota = device["rota"].as_bool().unwrap_or(true);
            let tran = device["tran"].as_str().unwrap_or("");
            
            let (block_class, tier) = if !rota {
                if tran == "nvme" {
                    ("nvme", "hot")
                } else {
                    ("ssd", "warm")
                }
            } else {
                ("hdd", "cold")
            };

            let children = if let Some(children) = device["children"].as_array() {
                children.iter().map(|child| {
                    Partition {
                        path: child["path"].as_str().unwrap_or("").to_string(),
                        fstype: child["fstype"].as_str().unwrap_or("").to_string(),
                        mountpoint: child["mountpoint"].as_str().unwrap_or("").to_string(),
                    }
                }).collect()
            } else {
                Vec::new()
            };

            Ok(BlockDevice {
                path: block_device.path.clone(),
                serial,
                block_class: block_class.to_string(),
                tier: tier.to_string(),
                children,
            })
        } else {
            Err(anyhow::anyhow!("Failed to parse block device information"))
        }
    }

    fn get_block_devices(&self) -> Result<Vec<BlockDevice>> {
        let output = self.run_command("lsblk", &["-dno", "path,type", "--json"])?;
        let v: Value = serde_json::from_str(&output)?;
        
        let mut block_devices = Vec::new();
        if let Some(devices) = v["blockdevices"].as_array() {
            for device in devices {
                if device["type"] == "disk" {
                    let path = device["path"].as_str().unwrap().to_string();
                    let block_device = self.update_block_device(&BlockDevice {
                        path,
                        serial: String::new(),
                        block_class: String::new(),
                        tier: String::new(),
                        children: Vec::new(),
                    })?;
                    block_devices.push(block_device);
                }
            }
        }
        Ok(block_devices)
    }

    fn setup_mergerfs(&self, active_block_devices: &[BlockDevice]) -> Result<()> {
        let mergerfs_opts = [
            "allow_other",
            "cache.files=auto-full",
            "dropcacheonclose=true",
            "category.create=mfs",
        ];

        let mount_points: Vec<String> = active_block_devices.iter()
            .map(|device| device.children[0].mountpoint.clone())
            .collect();

        let source = mount_points.join(":");
        let target = "/mnt/merged";

        fs::create_dir_all(target)?;

        let mut args = vec!["-o", &mergerfs_opts.join(","), &source, target];
        if self.dryrun {
            args.insert(0, "-f");
        }

        self.run_command("mergerfs", &args)?;

        Ok(())
    }
}

impl TieringManager {
    fn new(drive_manager: Arc<Mutex<DriveManager>>) -> Self {
        let db_path = "/etc/drive-manager/file_metadata.db".to_string();
        let tiering_manager = TieringManager { drive_manager, db_path };
        tiering_manager.setup_database().expect("Failed to set up database");
        tiering_manager
    }

    fn setup_database(&self) -> SqliteResult<()> {
        let conn = Connection::open(&self.db_path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS file_metadata (
                file_path TEXT PRIMARY KEY,
                last_access_time INTEGER,
                access_count INTEGER,
                last_tier_move INTEGER,
                file_size INTEGER
            )",
            [],
        )?;
        Ok(())
    }

    fn start_background_process(&self) {
        let tiering_manager = Arc::new(self.clone());
        
        // Tiering check loop
        let tm_clone = Arc::clone(&tiering_manager);
        thread::spawn(move || {
            loop {
                if let Err(e) = tm_clone.perform_tiering_check() {
                    error!("Error during tiering check: {:?}", e);
                }
                thread::sleep(Duration::from_secs(TIERING_CHECK_INTERVAL));
            }
        });

        // File mover loop
        let (sender, receiver) = bounded::<FileInfo>(100);
        let tm_clone = Arc::clone(&tiering_manager);
        thread::spawn(move || {
            tm_clone.file_mover_loop(receiver);
        });

        // Retry loop
        let (retry_sender, retry_receiver) = bounded::<FileInfo>(100);
        let tm_clone = Arc::clone(&tiering_manager);
        thread::spawn(move || {
            tm_clone.retry_loop(retry_receiver, sender);
        });
    }

    fn perform_tiering_check(&self) -> Result<()> {
        info!("Starting tiering check");
        self.update_file_metadata()?;
        self.check_tier_capacities()?;
        self.move_files_based_on_rules()?;
        info!("Tiering check completed");
        Ok(())
    }

    fn update_file_metadata(&self) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;
        
        for tier in &["hot", "warm", "cold"] {
            let tier_path = PathBuf::from("/mnt/merged").join(tier);
            for entry in WalkDir::new(tier_path) {
                let entry = entry?;
                if entry.file_type().is_file() {
                    let file_path = entry.path().to_str().unwrap().to_string();
                    let metadata = entry.metadata()?;
                    let atime = metadata.accessed()?.duration_since(UNIX_EPOCH)?.as_secs();
                    let size = metadata.len();

                    conn.execute(
                        "INSERT OR REPLACE INTO file_metadata 
                         (file_path, last_access_time, access_count, file_size) 
                         VALUES (?1, ?2, COALESCE((SELECT access_count FROM file_metadata WHERE file_path = ?1), 0) + 1, ?3)",
                        params![file_path, atime, size],
                    )?;
                }
            }
        }

        Ok(())
    }

    fn check_tier_capacities(&self) -> Result<()> {
        for tier in &["hot", "warm", "cold"] {
            let tier_path = PathBuf::from("/mnt/merged").join(tier);
            let stats = fs2::statvfs(&tier_path)?;
            let usage_percent = (stats.blocks() - stats.blocks_free()) as f64 / stats.blocks() as f64 * 100.0;

            if usage_percent > self.drive_manager.lock().unwrap().config.tier_capacity_threshold {
                self.move_files_down(tier)?;
            }
        }
        Ok(())
    }

    fn move_files_down(&self, source_tier: &str) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;
        let target_tier = if source_tier == "hot" { "warm" } else { "cold" };

        let mut stmt = conn.prepare(
            "SELECT file_path FROM file_metadata 
             WHERE file_path LIKE ? 
             ORDER BY last_access_time ASC LIMIT 10"
        )?;

        let files_to_move: Vec<String> = stmt.query_map(
            params![format!("/mnt/merged/{}/%", source_tier)],
            |row| row.get(0)
        )?.filter_map(|r| r.ok()).collect();

        for file_path in files_to_move {
            self.queue_file_move(file_path, source_tier.to_string(), target_tier.to_string())?;
        }

        Ok(())
    }

    fn move_files_based_on_rules(&self) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;
        let config = &self.drive_manager.lock().unwrap().config;
        let access_time_threshold = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() - config.access_time_threshold;

        let mut stmt = conn.prepare(
            "SELECT file_path FROM file_metadata 
             WHERE access_count >= ? AND last_access_time > ? 
             AND file_path NOT LIKE '/mnt/merged/hot/%'"
        )?;

        let files_to_move_up: Vec<String> = stmt.query_map(
            params![config.access_count_threshold, access_time_threshold],
            |row| row.get(0)
        )?.filter_map(|r| r.ok()).collect();

        for file_path in files_to_move_up {
            let current_tier = if file_path.contains("/warm/") { "warm" } else { "cold" };
            self.queue_file_move(file_path, current_tier.to_string(), "hot".to_string())?;
        }

        Ok(())
    }

    fn queue_file_move(&self, file_path: String, source_tier: String, target_tier: String) -> Result<()> {
        // In a real implementation, you'd send this to a channel
        // For simplicity, we'll just log it here
        info!("Queueing file move: {} from {} to {}", file_path, source_tier, target_tier);
        Ok(())
    }

fn file_mover_loop(&self, receiver: Receiver<FileInfo>) {
        loop {
            match receiver.recv() {
                Ok(file_info) => {
                    if let Err(e) = self.move_file(&file_info) {
                        error!("Failed to move file: {:?}. Error: {:?}", file_info, e);
                        // In a real implementation, you'd send this to a retry channel
                    }
                }
                Err(e) => {
                    error!("Error receiving file info: {:?}", e);
                    break;
                }
            }
        }
    }

    fn retry_loop(&self, retry_receiver: Receiver<FileInfo>, sender: Sender<FileInfo>) {
        loop {
            match retry_receiver.recv_timeout(Duration::from_secs(60)) {
                Ok(mut file_info) => {
                    if file_info.retries < 3 {
                        file_info.retries += 1;
                        if let Err(e) = sender.send(file_info.clone()) {
                            error!("Failed to send file for retry: {:?}. Error: {:?}", file_info, e);
                        }
                    } else {
                        error!("Failed to move file after 3 retries: {:?}", file_info);
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(e) => {
                    error!("Error receiving retry file info: {:?}", e);
                    break;
                }
            }
        }
    }

    fn move_file(&self, file_info: &FileInfo) -> Result<()> {
        let source_path = PathBuf::from("/mnt/merged").join(&file_info.source_tier);
        let target_path = PathBuf::from("/mnt/merged").join(&file_info.target_tier);
        let relative_path = PathBuf::from(&file_info.src).strip_prefix(&source_path)?;
        let dest = target_path.join(relative_path);

        fs::create_dir_all(dest.parent().unwrap())?;

        if self.rsync(&file_info.src, &dest.to_string_lossy())? {
            info!("Moved file from {} to {}", file_info.src, dest.display());

            let conn = Connection::open(&self.db_path)?;
            conn.execute(
                "UPDATE file_metadata SET file_path = ?, last_tier_move = ? WHERE file_path = ?",
                params![dest.to_string_lossy(), SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(), file_info.src],
            )?;

            Ok(())
        } else {
            Err(anyhow::anyhow!("Failed to move file"))
        }
    }

    fn rsync(&self, src: &str, dest: &str) -> Result<bool> {
        let output = Command::new("rsync")
            .args(&["-axqHAXWES", "--preallocate", "--remove-source-files", src, dest])
            .output()?;

        Ok(output.status.success())
    }
}

#[derive(Debug, Clone)]
struct FileInfo {
    src: String,
    source_tier: String,
    target_tier: String,
    retries: u32,
}

fn main() -> Result<()> {
    env_logger::init();

    let matches = App::new("Drive Manager")
        .version("1.0")
        .author("Your Name")
        .about("Manages drives and implements tiering")
        .arg(Arg::with_name("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .takes_value(true))
        .arg(Arg::with_name("dryrun")
            .long("dryrun")
            .help("Run in dry-run mode"))
        .get_matches();

    let config_path = matches.value_of("config").unwrap_or(CONFIG_FILE_PATH);
    let dryrun = matches.is_present("dryrun");

    let drive_manager = Arc::new(Mutex::new(DriveManager::new(config_path, dryrun)?));
    let tiering_manager = TieringManager::new(Arc::clone(&drive_manager));

    let block_devices = drive_manager.lock().unwrap().get_block_devices()?;
    let mut active_drives = Vec::new();

    for block_device in block_devices {
        let serial = &block_device.serial;
        let path = &block_device.path;
        let block_class = &block_device.block_class;

        if drive_manager.lock().unwrap().config.exclude_drives.contains(serial) {
            info!("{} {} to be excluded", path, serial);
        } else if block_device.children.len() == 1 && block_device.children[0].fstype == drive_manager.lock().unwrap().config.filesystem {
            info!("{} {} to be mounted as {}", path, serial, block_class);
            active_drives.push(drive_manager.lock().unwrap().mount_drive(&block_device)?);
        } else {
            info!("{} {} to be formatted as {}", path, serial, block_class);
            active_drives.push(drive_manager.lock().unwrap().format_drive(&block_device)?);
        }
    }

    drive_manager.lock().unwrap().setup_mergerfs(&active_drives)?;
    tiering_manager.start_background_process();

    // Keep the main thread running
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
