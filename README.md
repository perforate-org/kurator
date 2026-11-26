# Kurator

独立運営者による P2P マルチモデルデータベース(作業中)。

現在は Phase 1。

- Phase 0: 単一ノードの Memory-first Storage(KV)、独自の追記型 WAL
  (group commit、fsync、破損末尾の検出と切り詰め)、Snapshot + WAL 再生による
  クラッシュ復旧。
- Phase 1: OpenRaft による単一シャード 3 ノード複製(fsync 後にだけ数える
  quorum commit、リーダー選出、ログ追従、スナップショット転送)と、署名付き WAL
  (各レコードが直前レコードのダイジェストと Ed25519 署名を持つハッシュチェーン)。
  決定論的シミュレーションで遅延・消失・分断・クラッシュ・再起動を注入して検証する。

| crate | 役割 |
| --- | --- |
| `crates/kurator-codec` | 共通のバイナリ符号化(`Reader`、ファイル枠 `Format`) |
| `crates/kurator-wal` | 追記型 WAL。チェックサム版と署名版 |
| `crates/kurator-storage` | メモリ上の状態、group commit、スナップショット、`Db` |
| `crates/kurator-raft` | Raft ログストア・状態機械・スナップショット・`Replica` |
| `crates/kurator-sim` | 決定論的シミュレーション(シード付きネットワークと障害) |

シミュレーションは既定で 12 シード。`KURATOR_SIM_SEEDS=500 cargo test -p kurator-sim --release`
で増やし、失敗したシードは `KURATOR_SIM_SEED=<seed>` で再現する。

開発の決まりと品質ゲートは [CLAUDE.md](CLAUDE.md) を参照。
