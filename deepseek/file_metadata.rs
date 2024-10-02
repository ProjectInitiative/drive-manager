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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_move_info_clone() {
        let file_info = FileMoveInfo {
            src: "test_file".to_string(),
            source_tier: "hot".to_string(),
            target_tier: "warm".to_string(),
            retries: 0,
        };
        let cloned_info = file_info.clone();
        assert_eq!(file_info.src, cloned_info.src);
        assert_eq!(file_info.source_tier, cloned_info.source_tier);
        assert_eq!(file_info.target_tier, cloned_info.target_tier);
        assert_eq!(file_info.retries, cloned_info.retries);
    }

    #[test]
    fn test_file_metadata_clone() {
        let file_metadata = FileMetadata {
            last_access_time: SystemTime::now(),
            access_count: 1,
            file_size: 1024,
            tier: "hot".to_string(),
        };
        let cloned_metadata = file_metadata.clone();
        assert_eq!(file_metadata.last_access_time, cloned_metadata.last_access_time);
        assert_eq!(file_metadata.access_count, cloned_metadata.access_count);
        assert_eq!(file_metadata.file_size, cloned_metadata.file_size);
        assert_eq!(file_metadata.tier, cloned_metadata.tier);
    }
}
