use std::collections::BTreeMap;
use std::time::SystemTime;

use chrono::{DateTime, Utc};

use crate::base::{parse_snapshot_path, parse_wal_segment_path};
use crate::error::{Error, Result};
use crate::storage::SnapshotInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestPlannerWalPack {
    pub(crate) start_lsn: u64,
    pub(crate) end_lsn: u64,
    pub(crate) object_key: String,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
    pub(crate) lineage_id: String,
    pub(crate) base_snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestPlannerInput {
    pub(crate) generation: String,
    pub(crate) manifest_id: String,
    pub(crate) lineage_id: String,
    pub(crate) base_snapshot_id: String,
    pub(crate) base_snapshot_sha256: String,
    pub(crate) base_snapshot: String,
    pub(crate) wal_packs: Vec<ManifestPlannerWalPack>,
}

#[derive(Debug, Clone)]
pub(crate) struct ManifestRestoreWalObject {
    pub(crate) index: u64,
    pub(crate) offset: u64,
    pub(crate) end_offset: u64,
    pub(crate) object_key: String,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ManifestRestorePlan {
    pub(crate) snapshot: SnapshotInfo,
    pub(crate) snapshot_key: String,
    pub(crate) snapshot_sha256: String,
    pub(crate) wal_objects: Vec<ManifestRestoreWalObject>,
}

pub(crate) fn plan_manifest_restore(input: &ManifestPlannerInput) -> Result<ManifestRestorePlan> {
    let generation = crate::base::Generation::try_create(&input.generation)?;
    let generation_marker = format!("/generations/{}/", input.generation);
    if !input.base_snapshot.contains(&generation_marker) {
        return Err(Error::StorageError(format!(
            "base snapshot path {} does not belong to generation {}",
            input.base_snapshot, input.generation
        )));
    }

    let (snapshot_index, snapshot_offset) = parse_snapshot_path(&input.base_snapshot)?;
    let snapshot = SnapshotInfo {
        generation,
        index: snapshot_index,
        offset: snapshot_offset,
        size: 0,
        created_at: DateTime::<Utc>::from(SystemTime::UNIX_EPOCH),
    };

    if input.wal_packs.is_empty() {
        return Ok(ManifestRestorePlan {
            snapshot,
            snapshot_key: input.base_snapshot.clone(),
            snapshot_sha256: input.base_snapshot_sha256.clone(),
            wal_objects: vec![],
        });
    }

    let mut packs = input
        .wal_packs
        .iter()
        .map(|pack| {
            let (index, offset) = parse_wal_segment_path(&pack.object_key)?;
            Ok((pack.clone(), index, offset))
        })
        .collect::<Result<Vec<_>>>()?;
    packs.sort_by(|a, b| {
        a.1.cmp(&b.1)
            .then(a.2.cmp(&b.2))
            .then(a.0.end_lsn.cmp(&b.0.end_lsn))
            .then(a.0.object_key.cmp(&b.0.object_key))
    });

    let mut expected_index = snapshot.index;
    let mut expected_offset = 0;
    let mut wal_segments_by_index: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut wal_objects: Vec<ManifestRestoreWalObject> = Vec::with_capacity(packs.len());
    let mut seen_pack_starts: BTreeMap<(u64, u64), ManifestPlannerWalPack> = BTreeMap::new();

    for (pack, index, offset) in packs {
        if !pack.object_key.contains(&generation_marker) {
            return Err(Error::StorageError(format!(
                "wal pack path {} does not belong to generation {}",
                pack.object_key, input.generation
            )));
        }
        if pack.lineage_id != input.lineage_id {
            return Err(Error::StorageError(format!(
                "wal pack lineage mismatch: expected {}, got {}",
                input.lineage_id, pack.lineage_id
            )));
        }
        if pack.base_snapshot_id != input.base_snapshot_id {
            return Err(Error::StorageError(format!(
                "wal pack base snapshot mismatch: expected {}, got {}",
                input.base_snapshot_id, pack.base_snapshot_id
            )));
        }
        if pack.end_lsn < pack.start_lsn {
            return Err(Error::StorageError(format!(
                "wal pack end_lsn {} precedes start_lsn {}",
                pack.end_lsn, pack.start_lsn
            )));
        }
        if let Some(existing) = seen_pack_starts.get(&(index, offset)) {
            let exact_duplicate = existing.start_lsn == pack.start_lsn
                && existing.end_lsn == pack.end_lsn
                && existing.object_key == pack.object_key
                && existing.sha256 == pack.sha256
                && existing.size_bytes == pack.size_bytes
                && existing.lineage_id == pack.lineage_id
                && existing.base_snapshot_id == pack.base_snapshot_id;
            if exact_duplicate {
                continue;
            }
            return Err(Error::StorageError(format!(
                "conflicting wal packs share same start {}:{}",
                index, offset
            )));
        }

        let expected_key_position = if index == expected_index {
            offset == expected_offset
        } else {
            index == expected_index + 1 && offset == 0
        };
        if !expected_key_position {
            return Err(Error::StorageError(format!(
                "wal pack gap or overlap: expected index/offset {}:{}, got {}:{}",
                expected_index, expected_offset, index, offset
            )));
        }
        if pack.start_lsn != offset {
            return Err(Error::StorageError(format!(
                "wal pack start_lsn {} does not match object key offset {}",
                pack.start_lsn, offset
            )));
        }

        wal_segments_by_index.entry(index).or_default().push(offset);
        seen_pack_starts.insert((index, offset), pack.clone());
        wal_objects.push(ManifestRestoreWalObject {
            index,
            offset,
            end_offset: pack.end_lsn,
            object_key: pack.object_key,
            sha256: pack.sha256,
        });
        expected_index = index;
        expected_offset = pack.end_lsn;
    }

    wal_segments_by_index
        .into_iter()
        .map(|(index, mut offsets)| {
            offsets.sort_unstable();
            offsets.dedup();
            let expected_first_offset = 0;
            if offsets.first().copied().unwrap_or_default() != expected_first_offset {
                return Err(Error::StorageError(format!(
                    "wal pack offsets for index {} must start at {}",
                    index, expected_first_offset
                )));
            }
            Ok::<(), Error>(())
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ManifestRestorePlan {
        snapshot,
        snapshot_key: input.base_snapshot.clone(),
        snapshot_sha256: input.base_snapshot_sha256.clone(),
        wal_objects,
    })
}

#[cfg(test)]
mod tests {
    use super::{ManifestPlannerInput, ManifestPlannerWalPack, plan_manifest_restore};
    use crate::error::Error;

    fn sample_input() -> ManifestPlannerInput {
        ManifestPlannerInput {
            generation: "019c3e53aea47afbbddfe5ebc2272e22".to_string(),
            manifest_id: "manifest-01".to_string(),
            lineage_id: "lineage-01".to_string(),
            base_snapshot_id: "snapshot-01".to_string(),
            base_snapshot_sha256: "deadbeef".to_string(),
            base_snapshot:
                "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/snapshots/0000000001_0000000000.snapshot.zst"
                    .to_string(),
            wal_packs: vec![
                ManifestPlannerWalPack {
                    start_lsn: 0,
                    end_lsn: 1024,
                    object_key:
                        "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/wal/0000000001_0000000000.wal.zst"
                            .to_string(),
                    sha256: "sha-a".to_string(),
                    size_bytes: 128,
                    lineage_id: "lineage-01".to_string(),
                    base_snapshot_id: "snapshot-01".to_string(),
                },
                ManifestPlannerWalPack {
                    start_lsn: 0,
                    end_lsn: 1024,
                    object_key:
                        "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/wal/0000000002_0000000000.wal.zst"
                            .to_string(),
                    sha256: "sha-b".to_string(),
                    size_bytes: 128,
                    lineage_id: "lineage-01".to_string(),
                    base_snapshot_id: "snapshot-01".to_string(),
                },
            ],
        }
    }

    #[test]
    fn manifest_plan_selects_exact_snapshot_and_pack_chain() {
        let plan = plan_manifest_restore(&sample_input()).expect("manifest plan");

        assert_eq!(plan.snapshot.index, 1);
        assert_eq!(plan.snapshot.offset, 0);
        assert_eq!(
            plan.snapshot_key,
            "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/snapshots/0000000001_0000000000.snapshot.zst"
        );
        assert_eq!(plan.wal_objects.len(), 2);
        assert_eq!(
            plan.wal_objects[0].object_key,
            "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/wal/0000000001_0000000000.wal.zst"
        );
        assert_eq!(plan.wal_objects[0].end_offset, 1024);
        assert_eq!(
            plan.wal_objects[1].object_key,
            "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/wal/0000000002_0000000000.wal.zst"
        );
        assert_eq!(plan.wal_objects[1].end_offset, 1024);
    }

    #[test]
    fn manifest_plan_rejects_gap_in_pack_chain() {
        let mut input = sample_input();
        input.wal_packs[1].start_lsn = 2048;

        let err = plan_manifest_restore(&input).expect_err("gap should fail");
        assert_eq!(err.code(), Error::STORAGE_ERROR);
    }

    #[test]
    fn manifest_plan_rejects_cross_lineage_pack() {
        let mut input = sample_input();
        input.wal_packs[1].lineage_id = "lineage-02".to_string();

        let err = plan_manifest_restore(&input).expect_err("cross-lineage pack should fail");
        assert_eq!(err.code(), Error::STORAGE_ERROR);
    }

    #[test]
    fn manifest_plan_allows_next_index_to_restart_offset_at_zero() {
        let mut input = sample_input();
        input.base_snapshot =
            "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/snapshots/0000000001_0000004152.snapshot.zst"
                .to_string();
        input.wal_packs = vec![
            ManifestPlannerWalPack {
                start_lsn: 0,
                end_lsn: 16512,
                object_key:
                    "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/wal/ranges/0000000001_0000000000_0000000001_0000016512/0000000001_0000000000.wal.zst"
                        .to_string(),
                sha256: "sha-a".to_string(),
                size_bytes: 128,
                lineage_id: "lineage-01".to_string(),
                base_snapshot_id: "snapshot-01".to_string(),
            },
            ManifestPlannerWalPack {
                start_lsn: 0,
                end_lsn: 4096,
                object_key:
                    "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/wal/ranges/0000000002_0000000000_0000000002_0000004096/0000000002_0000000000.wal.zst"
                        .to_string(),
                sha256: "sha-b".to_string(),
                size_bytes: 128,
                lineage_id: "lineage-01".to_string(),
                base_snapshot_id: "snapshot-01".to_string(),
            },
        ];

        let _plan = plan_manifest_restore(&input).expect("manifest plan");
    }

    #[test]
    fn manifest_plan_rejects_conflicting_same_start_pack() {
        let mut input = sample_input();
        input.wal_packs.push(ManifestPlannerWalPack {
            start_lsn: 0,
            end_lsn: 2048,
            object_key:
                "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/wal/ranges/0000000001_0000000000_0000000001_0000002048/0000000001_0000000000.wal.zst"
                    .to_string(),
            sha256: "sha-conflict".to_string(),
            size_bytes: 256,
            lineage_id: "lineage-01".to_string(),
            base_snapshot_id: "snapshot-01".to_string(),
        });

        let err = plan_manifest_restore(&input)
            .expect_err("conflicting same-start wal packs should fail");
        assert_eq!(err.code(), Error::STORAGE_ERROR);
    }
}
