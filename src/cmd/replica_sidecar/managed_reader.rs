#![allow(dead_code)]

use std::path::{Path, PathBuf};

const DATA_DB_FILE: &str = "data.db";
const ACTIVE_LINK: &str = "active";
const GENERATIONS_DIR: &str = "generations";

#[derive(Clone, Debug)]
pub(super) struct ManagedChildTemplate {
    raw: String,
}

impl ManagedChildTemplate {
    pub(super) fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("managed child template cannot be empty".to_string());
        }
        let mut missing = Vec::new();
        if !trimmed.contains("{port}") {
            missing.push("{port}");
        }
        if !trimmed.contains("{dir}") {
            missing.push("{dir}");
        }
        if !missing.is_empty() {
            return Err(format!(
                "managed child template missing required placeholders: {}",
                missing.join(", ")
            ));
        }
        Ok(Self {
            raw: trimmed.to_string(),
        })
    }

    pub(super) fn render(&self, port: u16, generation_dir: &Path) -> String {
        let generation_dir = generation_dir.to_string_lossy();
        let db_path = Path::new(generation_dir.as_ref()).join(DATA_DB_FILE);
        self.raw
            .replace("{port}", &port.to_string())
            .replace("{dir}", generation_dir.as_ref())
            .replace("{db}", &db_path.to_string_lossy())
    }
}

pub(super) struct ManagedGenerationLayout {
    root: PathBuf,
}

impl ManagedGenerationLayout {
    pub(super) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub(super) fn generation_dir(&self, generation_id: &str) -> PathBuf {
        self.root.join(GENERATIONS_DIR).join(generation_id)
    }

    pub(super) fn data_db_path(&self, generation_id: &str) -> PathBuf {
        self.generation_dir(generation_id).join(DATA_DB_FILE)
    }

    pub(super) fn active_link_path(&self) -> PathBuf {
        self.root.join(ACTIVE_LINK)
    }

    #[cfg(unix)]
    pub(super) fn promote_active_generation(&self, generation_id: &str) -> std::io::Result<()> {
        let generation_dir = self.generation_dir(generation_id);
        if !generation_dir.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "generation directory not found: {}",
                    generation_dir.display()
                ),
            ));
        }

        std::fs::create_dir_all(&self.root)?;
        let active_link = self.active_link_path();
        let tmp_link = self.root.join(format!(".{ACTIVE_LINK}.tmp"));
        match std::fs::remove_file(&tmp_link) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        std::os::unix::fs::symlink(&generation_dir, &tmp_link)?;
        std::fs::rename(&tmp_link, &active_link)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use super::{ManagedChildTemplate, ManagedGenerationLayout};

    #[test]
    fn child_template_renders_port_dir_and_db_placeholders() {
        let template =
            ManagedChildTemplate::parse("/app serve --http 127.0.0.1:{port} --dir {dir} --db {db}")
                .expect("parse template");

        let rendered = template.render(38090, Path::new("/pb_data/gen/a"));

        assert_eq!(
            rendered,
            "/app serve --http 127.0.0.1:38090 --dir /pb_data/gen/a --db /pb_data/gen/a/data.db"
        );
    }

    #[test]
    fn child_template_requires_port_and_dir_placeholders() {
        let err = ManagedChildTemplate::parse("/app serve --http 127.0.0.1:8090 --dir /pb_data")
            .expect_err("template without placeholders should fail");

        assert!(err.contains("{port}"));
        assert!(err.contains("{dir}"));
    }

    #[test]
    fn generation_layout_builds_stable_paths() {
        let layout = ManagedGenerationLayout::new("/pb_data/replited");

        assert_eq!(
            layout.generation_dir("gen-a"),
            Path::new("/pb_data/replited/generations/gen-a")
        );
        assert_eq!(
            layout.data_db_path("gen-a"),
            Path::new("/pb_data/replited/generations/gen-a/data.db")
        );
        assert_eq!(
            layout.active_link_path(),
            Path::new("/pb_data/replited/active")
        );
    }

    #[cfg(unix)]
    #[test]
    fn promote_active_generation_atomically_repoints_active_symlink() {
        let dir = tempdir().expect("tempdir");
        let layout = ManagedGenerationLayout::new(dir.path());
        std::fs::create_dir_all(layout.generation_dir("gen-a")).expect("create gen-a");
        std::fs::create_dir_all(layout.generation_dir("gen-b")).expect("create gen-b");

        layout
            .promote_active_generation("gen-a")
            .expect("promote gen-a");
        assert_eq!(
            std::fs::read_link(layout.active_link_path()).expect("read active"),
            layout.generation_dir("gen-a")
        );

        layout
            .promote_active_generation("gen-b")
            .expect("promote gen-b");
        assert_eq!(
            std::fs::read_link(layout.active_link_path()).expect("read active"),
            layout.generation_dir("gen-b")
        );
    }

    #[cfg(unix)]
    #[test]
    fn promote_active_generation_rejects_missing_generation() {
        let dir = tempdir().expect("tempdir");
        let layout = ManagedGenerationLayout::new(dir.path());

        let err = layout
            .promote_active_generation("missing")
            .expect_err("missing generation should fail");

        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert!(!layout.active_link_path().exists());
    }
}
