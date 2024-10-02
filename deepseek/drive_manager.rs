use std::fs;
use std::path::Path;
use std::process::Command;
use serde_json::Value;
use log::{info, error};
use crate::args::Args;
use crate::tiering_manager::TieringManager;

pub struct DriveManager {
    args: Args,
    config: Value,
    new_drive_mounted: bool,
    tiering_manager: TieringManager,
}

impl DriveManager {
    pub fn new(args: Args) -> Self {
        let config = Self::read_config(&args);
        let new_drive_mounted = false;
        let tiering_manager = TieringManager::new(args.clone());
        Self { args, config, new_drive_mounted, tiering_manager }
    }

    pub fn run_command(&self, cmd: &[&str]) -> Result<(), std::io::Error> {
        if self.args.dryrun {
            info!("DRYRUN: {}", cmd.join(" "));
            return Ok(());
        }
        info!("{}", cmd.join(" "));
        Command::new(cmd[0]).args(&cmd[1..]).status()?;
        Ok(())
    }

    pub fn get_atime(directory: &str) -> HashMap<String, SystemTime> {
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

    pub fn rsync(&self, src: &str, dest: &str) -> bool {
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

    pub fn sort_block_device(&self, block_device: &Value) -> i32 {
        let class_order = HashMap::from([("nvme", 0), ("ssd", 1), ("hdd", 2)]);
        let block_class = block_device["block_class"].as_str().unwrap();
        *class_order.get(block_class).unwrap()
    }

    pub fn setup_mergerfs(&self, active_block_devices: Vec<Value>) {
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

    pub fn mount_drive(&mut self, block_device: &Value) -> Value {
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

    pub fn format_drive(&mut self, block_device: &Value) -> Value {
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

    pub fn read_config(args: &Args) -> Value {
        let config_file = fs::read_to_string(args.config.clone()).unwrap();
        serde_json::from_str(&config_file).unwrap()
    }

    pub fn update_block_device(&self, block_device: &Value) -> Value {
        let output = Command::new("lsblk").args(Self::LSBLK_DISCOVER_CMD).arg(block_device["path"].as_str().unwrap()).output().unwrap();
        let block_device: Value = serde_json::from_str(&String::from_utf8_lossy(&output.stdout)).unwrap();
        self.classify_block_class(&mut block_device);
        block_device
    }

    pub fn classify_block_class(&self, block_device: &mut Value) {
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

    pub fn get_block_devices(&self) -> Vec<Value> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_run_command() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let drive_manager = DriveManager::new(args);
        let result = drive_manager.run_command(&["echo", "test"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_atime() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "test data").unwrap();
        let atime_dict = DriveManager::get_atime(dir.path().to_str().unwrap());
        assert!(atime_dict.contains_key(file_path.to_str().unwrap()));
    }

    #[test]
    fn test_rsync() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let drive_manager = DriveManager::new(args);
        let result = drive_manager.rsync("/src", "/dest");
        assert!(result);
    }

    #[test]
    fn test_sort_block_device() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let drive_manager = DriveManager::new(args);
        let block_device = json!({ "block_class": "nvme" });
        let result = drive_manager.sort_block_device(&block_device);
        assert_eq!(result, 0);
    }

    #[test]
    fn test_mount_drive() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let mut drive_manager = DriveManager::new(args);
        let block_device = json!({ "block_class": "nvme", "serial": "1234", "children": [{ "path": "/dev/sda1", "mountpoint": "/mnt/physical/nvme/1234" }] });
        let result = drive_manager.mount_drive(&block_device);
        assert!(result.is_object());
    }

    #[test]
    fn test_format_drive() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let mut drive_manager = DriveManager::new(args);
        let block_device = json!({ "path": "/dev/sda", "children": [] });
        let result = drive_manager.format_drive(&block_device);
        assert!(result.is_object());
    }

    #[test]
    fn test_get_block_devices() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let drive_manager = DriveManager::new(args);
        let result = drive_manager.get_block_devices();
        assert!(result.is_empty());
    }
}
