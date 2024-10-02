mod drive_manager;
mod tiering_manager;
mod args;
mod file_metadata;

use drive_manager::DriveManager;
use args::Args;
use log::info;
use simple_logger::SimpleLogger;
use std::time::Duration;
use std::thread;

fn main() {
    SimpleLogger::new().init().unwrap();
    let args = Args::parse();
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
