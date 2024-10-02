use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use log::{info, error};
use crate::args::Args;
use crate::file_metadata::{FileMoveInfo, FileMetadata};

pub struct TieringManager {
    args: Args,
    db_path: String,
    db: shelve::Shelf,
    move_queue: mpsc::Sender<FileMoveInfo>,
    retry_queue: mpsc::Sender<FileMoveInfo>,
    executor: threadpool::ThreadPool,
}

impl TieringManager {
    pub fn new(args: Args) -> Self {
        let db_path = "/etc/drive-manager/file_metadata.db".to_string();
        let db = shelve::open(&db_path).unwrap();
        let (move_tx, move_rx) = mpsc::channel();
        let (retry_tx, retry_rx) = mpsc::channel();
        let executor = threadpool::ThreadPool::new(args.threads);
        Self { args, db_path, db, move_queue: move_tx, retry_queue: retry_tx, executor }
    }

    pub fn start_background_process(&self) {
        thread::spawn(move || self.tiering_check_loop());
        thread::spawn(move || self.file_mover_loop(move_rx));
        thread::spawn(move || self.retry_loop(retry_rx));
        thread::spawn(move || self.maintenance_loop());
    }

    pub fn tiering_check_loop(&self) {
        loop {
            self.perform_tiering_check();
            thread::sleep(Duration::from_secs(TIERING_CHECK_INTERVAL));
        }
    }

    pub fn file_mover_loop(&self, rx: Receiver<FileMoveInfo>) {
        for file_info in rx {
            self.executor.execute(move || self.move_file(file_info));
        }
    }

    pub fn retry_loop(&self, rx: Receiver<FileMoveInfo>) {
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

    pub fn perform_tiering_check(&self) {
        info!("Starting tiering check");
        self.update_file_metadata();
        self.check_tier_capacities();
        self.move_files_based_on_rules();
        info!("Tiering check completed");
    }

    pub fn update_file_metadata(&self) {
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

    pub fn check_tier_capacities(&self) {
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

    pub fn move_files_down(&self, source_tier: &str) {
        let target_tier = if source_tier == "hot" { "warm" } else { "cold" };
        let files_to_move: Vec<_> = self.db.iter().filter(|(_, file_info)| file_info.tier == source_tier).collect();
        for (file_path, _) in files_to_move.iter().take(10) {
            self.queue_file_move(file_path.clone(), source_tier.to_string(), target_tier.to_string());
        }
    }

    pub fn move_files_based_on_rules(&self) {
        let access_time_threshold = SystemTime::now() - Duration::from_secs(self.drive_manager.lock().unwrap().config.get("access_time_threshold").unwrap().as_u64().unwrap());
        let access_count_threshold = self.drive_manager.lock().unwrap().config.get("access_count_threshold").unwrap().as_u64().unwrap();
        for (file_path, file_info) in self.db.iter() {
            if file_info.access_count >= access_count_threshold && file_info.last_access_time > access_time_threshold && file_info.tier != "hot" {
                self.queue_file_move(file_path.clone(), file_info.tier.clone(), "hot".to_string());
            }
        }
    }

    pub fn queue_file_move(&self, file_path: String, source_tier: String, target_tier: String) {
        self.move_queue.send(FileMoveInfo {
            src: file_path,
            source_tier,
            target_tier,
            retries: 0,
        }).unwrap();
    }

    pub fn move_file(&self, file_info: FileMoveInfo) {
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

    pub fn validate_and_update_database(&self) {
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

    pub fn maintenance_loop(&self) {
        loop {
            self.validate_and_update_database();
            thread::sleep(Duration::from_secs(86400));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_update_file_metadata() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let tiering_manager = TieringManager::new(args);
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "test data").unwrap();
        tiering_manager.update_file_metadata();
        assert!(tiering_manager.db.contains_key(file_path.to_str().unwrap()));
    }

    #[test]
    fn test_check_tier_capacities() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let tiering_manager = TieringManager::new(args);
        tiering_manager.check_tier_capacities();
        // Add assertions based on expected behavior
    }

    #[test]
    fn test_move_files_down() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let tiering_manager = TieringManager::new(args);
        tiering_manager.move_files_down("hot");
        // Add assertions based on expected behavior
    }

    #[test]
    fn test_move_files_based_on_rules() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let tiering_manager = TieringManager::new(args);
        tiering_manager.move_files_based_on_rules();
        // Add assertions based on expected behavior
    }

    #[test]
    fn test_validate_and_update_database() {
        let args = Args { dryrun: true, config: "".to_string(), threads: 4 };
        let tiering_manager = TieringManager::new(args);
        tiering_manager.validate_and_update_database();
        // Add assertions based on expected behavior
    }
}
