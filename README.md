# Kurator

独立運営者による P2P マルチモデルデータベース(作業中)。

現在は Phase 0: 単一ノードの Memory-first Storage(KV)、独自の追記型 WAL(group commit, fsync, 破損末尾の検出と切り詰め)、Snapshot + WAL 再生によるクラッシュ復旧。

- `crates/kurator-wal`: 追記型 WAL
- `crates/kurator-storage`: メモリ上の状態、group commit、スナップショット、`Db`
