use std::path::PathBuf;

pub fn remove_file(path: &PathBuf) -> std::io::Result<()> {
    std::fs::remove_file(path)
}
