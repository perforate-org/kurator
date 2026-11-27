//! The node secret on disk: `<dir>/node.secret`, 32 random bytes that
//! only the owner can read.

use std::fs;
use std::io::{self, Write as _};
use std::path::Path;

use kurator_crypto::NodeSecret;

const FILE: &str = "node.secret";

/// Whether the secret was there or made just now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Origin {
    /// Read from the directory.
    Loaded,
    /// Generated and written to the directory.
    Created,
}

/// The secret in `dir`, created when there is none.
///
/// # Errors
///
/// When the file cannot be read or written, has the wrong length, or the
/// system has no randomness.
pub fn load_or_create(dir: &Path) -> io::Result<(NodeSecret, Origin)> {
    let path = dir.join(FILE);
    match fs::read(&path) {
        Ok(bytes) => {
            let seed: [u8; 32] = bytes.try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} is not 32 bytes", path.display()),
                )
            })?;
            Ok((NodeSecret::from_seed(seed), Origin::Loaded))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut seed = [0_u8; 32];
            getrandom::fill(&mut seed).map_err(io::Error::other)?;
            fs::create_dir_all(dir)?;
            let mut file = owner_only().open(&path)?;
            file.write_all(&seed)?;
            file.sync_all()?;
            Ok((NodeSecret::from_seed(seed), Origin::Created))
        }
        Err(error) => Err(error),
    }
}

/// Options that create a new file only the owner can read.
fn owner_only() -> fs::OpenOptions {
    let mut options = fs::OpenOptions::new();
    let _new = options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let _private = options.mode(0o600);
    }
    options
}

#[cfg(test)]
mod tests {
    use super::{FILE, Origin, load_or_create};

    #[test]
    fn a_secret_is_created_once_and_then_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let (created, first) = load_or_create(&dir.path().join("n1")).unwrap();
        assert_eq!(first, Origin::Created, "created");
        let (loaded, second) = load_or_create(&dir.path().join("n1")).unwrap();
        assert_eq!(second, Origin::Loaded, "loaded");
        assert_eq!(loaded, created, "the same secret");
        let (other, _) = load_or_create(&dir.path().join("n2")).unwrap();
        assert_ne!(other, created, "random");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.path().join("n1").join(FILE))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "owner only");
        }
        std::fs::write(dir.path().join("n2").join(FILE), b"short").unwrap();
        assert!(
            load_or_create(&dir.path().join("n2")).is_err(),
            "bad length"
        );
    }
}
