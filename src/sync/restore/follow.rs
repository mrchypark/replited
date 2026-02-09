use log::info;

use crate::error::Result;
use crate::storage::RestoreWalSegments;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;

fn new_generation_start_index(snapshot: &SnapshotInfo) -> u64 {
    snapshot.index
}

impl super::Restore {
    pub(super) async fn follow_loop(
        &self,
        mut current_client: StorageClient,
        mut current_snapshot: SnapshotInfo,
        mut last_index: u64,
        mut last_offset: u64,
    ) -> Result<()> {
        info!("entering follow mode...");
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(self.options.interval)).await;

            // 1. Check for new WAL segments in current generation
            let wal_segments = current_client
                .wal_segments(current_snapshot.generation.as_str())
                .await?;

            let mut new_segments = Vec::new();
            for segment in wal_segments {
                if segment.index > last_index
                    || (segment.index == last_index && segment.offset >= last_offset)
                {
                    new_segments.push(segment);
                }
            }

            if !new_segments.is_empty() {
                // Sort and group segments
                new_segments
                    .sort_by(|a, b| a.index.cmp(&b.index).then_with(|| a.offset.cmp(&b.offset)));

                let mut restore_wal_segments: std::collections::BTreeMap<u64, Vec<u64>> =
                    std::collections::BTreeMap::new();
                for segment in new_segments {
                    restore_wal_segments
                        .entry(segment.index)
                        .or_default()
                        .push(segment.offset);
                }
                let restore_wal_segments: RestoreWalSegments =
                    restore_wal_segments.into_iter().collect();

                let (new_last_index, new_last_offset, _) = self
                    .apply_wal_frames(
                        &current_client,
                        &current_snapshot,
                        &restore_wal_segments,
                        &self.options.output,
                        last_index,
                        last_offset,
                        false,
                    )
                    .await?;

                last_index = new_last_index;
                last_offset = new_last_offset;
                info!("applied updates up to index {last_index}, offset {last_offset}");
            }

            // 2. Check for new generations (restarts)
            if let Some((latest_info, client)) = self.decide_restore_info(None).await? {
                if latest_info.snapshot.generation > current_snapshot.generation {
                    info!(
                        "detected new generation: {:?}",
                        latest_info.snapshot.generation
                    );

                    current_client = client;
                    current_snapshot = latest_info.snapshot;

                    let (new_last_index, new_last_offset, _) = self
                        .apply_wal_frames(
                            &current_client,
                            &current_snapshot,
                            &latest_info.wal_segments,
                            &self.options.output,
                            new_generation_start_index(&current_snapshot),
                            0,
                            false,
                        )
                        .await?;
                    last_index = new_last_index;
                    last_offset = new_last_offset;
                    info!("switched to new generation and applied updates");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::base::Generation;
    use crate::storage::SnapshotInfo;

    #[test]
    fn new_generation_start_index_uses_snapshot_index() {
        let snapshot = SnapshotInfo {
            generation: Generation::new(),
            index: 17,
            offset: 8192,
            size: 0,
            created_at: chrono::Utc::now(),
        };

        assert_eq!(super::new_generation_start_index(&snapshot), 17);
    }
}
