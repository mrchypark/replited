use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LatestPointer {
    pub(crate) format_version: u32,
    pub(crate) current_generation: String,
    pub(crate) current_manifest_key: String,
    pub(crate) current_manifest_sha256: String,
    pub(crate) created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GenerationManifest {
    pub(crate) format_version: u32,
    pub(crate) generation: String,
    pub(crate) manifest_id: String,
    pub(crate) lineage_id: String,
    pub(crate) base_snapshot_id: String,
    pub(crate) base_snapshot_sha256: String,
    pub(crate) base_snapshot: String,
    pub(crate) wal_packs: Vec<ManifestWalPack>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManifestWalPack {
    pub(crate) start_lsn: u64,
    pub(crate) end_lsn: u64,
    pub(crate) object_key: String,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
    pub(crate) lineage_id: String,
    pub(crate) base_snapshot_id: String,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{GenerationManifest, LatestPointer, ManifestWalPack};

    #[test]
    fn manifest_latest_pointer_round_trip_json() {
        let pointer = LatestPointer {
            format_version: 1,
            current_generation: "gen-01".to_string(),
            current_manifest_key: "db/manifests/generations/gen-01.manifest.json".to_string(),
            current_manifest_sha256: "abc123".to_string(),
            created_at: Utc::now(),
        };

        let json = serde_json::to_string(&pointer).expect("serialize latest pointer");
        let decoded: LatestPointer =
            serde_json::from_str(&json).expect("deserialize latest pointer");

        assert_eq!(decoded.current_generation, pointer.current_generation);
        assert_eq!(decoded.current_manifest_key, pointer.current_manifest_key);
        assert_eq!(decoded.current_manifest_sha256, pointer.current_manifest_sha256);
    }

    #[test]
    fn manifest_generation_round_trip_json() {
        let manifest = GenerationManifest {
            format_version: 1,
            generation: "gen-01".to_string(),
            manifest_id: "manifest-01".to_string(),
            lineage_id: "lineage-01".to_string(),
            base_snapshot_id: "snapshot-01".to_string(),
            base_snapshot_sha256: "deadbeef".to_string(),
            base_snapshot: "db/snapshots/gen-01/snapshot-01.snapshot.zst".to_string(),
            wal_packs: vec![ManifestWalPack {
                start_lsn: 0,
                end_lsn: 4096,
                object_key: "db/wal/packs/gen-01/0_4096_pack-01.wal.zst".to_string(),
                sha256: "cafebabe".to_string(),
                size_bytes: 8192,
                lineage_id: "lineage-01".to_string(),
                base_snapshot_id: "snapshot-01".to_string(),
            }],
        };

        let json = serde_json::to_string(&manifest).expect("serialize generation manifest");
        let decoded: GenerationManifest =
            serde_json::from_str(&json).expect("deserialize generation manifest");

        assert_eq!(decoded.manifest_id, manifest.manifest_id);
        assert_eq!(decoded.base_snapshot, manifest.base_snapshot);
        assert_eq!(decoded.wal_packs, manifest.wal_packs);
    }

    #[test]
    fn manifest_pack_keeps_lineage_anchors() {
        let pack = ManifestWalPack {
            start_lsn: 8192,
            end_lsn: 16384,
            object_key: "db/wal/packs/gen-01/8192_16384_pack-02.wal.zst".to_string(),
            sha256: "facefeed".to_string(),
            size_bytes: 4096,
            lineage_id: "lineage-01".to_string(),
            base_snapshot_id: "snapshot-01".to_string(),
        };

        assert_eq!(pack.lineage_id, "lineage-01");
        assert_eq!(pack.base_snapshot_id, "snapshot-01");
    }
}
