use std::time::SystemTime;

#[derive(Clone)]
pub struct FileMoveInfo {
    pub src: String,
    pub source_tier: String,
    pub target_tier: String,
    pub retries: u32,
}

#[derive(Clone)]
pub struct FileMetadata {
    pub last_access_time: SystemTime,
    pub access_count: u64,
    pub file_size: u64,
    pub tier: String,
}
