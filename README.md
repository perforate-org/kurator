# Kurator

独立運営者による P2P マルチモデルデータベース(作業中)。

現在は Phase 3。

- Phase 0: 単一ノードの Memory-first Storage(KV)、独自の追記型 WAL
  (group commit、fsync、破損末尾の検出と切り詰め)、Snapshot + WAL 再生による
  クラッシュ復旧。
- Phase 1: OpenRaft による単一シャード 3 ノード複製(fsync 後にだけ数える
  quorum commit、リーダー選出、ログ追従、スナップショット転送)と、署名付き WAL
  (各レコードが直前レコードのダイジェストと Ed25519 署名を持つハッシュチェーン)。
  決定論的シミュレーションで遅延・消失・分断・クラッシュ・再起動を注入して検証する。
- Phase 2: 実ネットワーク。QUIC の永続接続でRaft RPC・ログ複製・スナップショット転送を
  独自バイナリ符号化で流し、TLS 1.3 の Raw Public Key(Ed25519)で相互認証する。
  メンバー変更は learner として追いつかせてから voter に昇格し、ノード除去もできる。
  QUIC とシミュレーションは同じ `kurator_raft::Transport` 境界で差し替わる。
- Phase 3: MVCC と単一シャードの直列化可能なトランザクション。各コマンドは版
  (単一ノードでは LSN、レプリカでは Raft エントリの index)で適用され、各キーは
  保持期間(既定 4096 版)ぶんの履歴を持つ。トランザクションは 1 つの版を読み、
  読んだキーと走査した範囲を記録し、書き込みを 1 つのログエントリとして提出する。
  エントリを適用する時点で「読んだ版より後に、読んだものへの書き込みがあったか」を
  検査して commit か abort を決める(楽観的並行性制御)。検査は適用側で行うので、
  どのレプリカもどの復旧も同じ判断になる。

| crate            | 役割                                                                 |
| ---------------- | -------------------------------------------------------------------- |
| `crates/codec`   | 共通のバイナリ符号化(`Reader`、ファイル枠 `Format`)                  |
| `crates/wal`     | 追記型 WAL。チェックサム版と署名版                                   |
| `crates/storage` | 版付きの状態、トランザクション、group commit、スナップショット、`Db` |
| `crates/raft`    | Raft ログストア・状態機械・スナップショット・`Replica`               |
| `crates/sim`     | 決定論的シミュレーション(シード付きネットワークと障害、参加・離脱)   |
| `crates/net`     | QUIC トランスポート(quinn、Ed25519 Raw Public Key の相互認証)        |
| `crates/node`    | `kurator` バイナリ: レプリカ 1 つをプロセスとして動かすコンソール    |

シミュレーションは既定で 12 シード。`KURATOR_SIM_SEEDS=500 cargo test -p kurator-sim --release`
で増やし、失敗したシードは `KURATOR_SIM_SEED=<seed>` で再現する。

## ローカルで 3 ノードを動かす

各ノードはディレクトリに秘密 `node.secret` を持ち、そこから WAL 署名鍵と通信鍵を
用途別に導出する。`kurator key` は秘密を作り(既にあれば読み)、通信鍵の公開鍵を表示する。

```sh
cargo build -p kurator-node
K=target/debug/kurator
K1=$($K key n1); K2=$($K key n2); K3=$($K key n3)
P1=1=127.0.0.1:7001,$K1; P2=2=127.0.0.1:7002,$K2; P3=3=127.0.0.1:7003,$K3

# 端末をノードごとに分けて起動する。--found を付けた 1 台がクラスタを作る。
$K run --id 1 --dir n1 --listen 127.0.0.1:7001 --peer $P2 --peer $P3 --found
$K run --id 2 --dir n2 --listen 127.0.0.1:7002 --peer $P1 --peer $P3
$K run --id 3 --dir n3 --listen 127.0.0.1:7003 --peer $P1 --peer $P2
```

起動したノードは標準入力からコマンドを読む。書き込みと読み出しはリーダーで行う
(`status` でリーダーが分かる)。

```text
put <key> <value> | del <key> | get <key> | status | members
add <id> <addr> <key> | promote <id> | remove <id> | quit
```

4 台目を足すには、既存ノードを `--peer` に並べて `--found` なしで起動し、リーダーで
`add 4 127.0.0.1:7004 <4 の公開鍵>` を実行する。スナップショットとログで追いついた後に
`promote 4` が通る(追いつく前は `cannot vote yet` で断られる)。`remove <id>` で抜く。
再起動は同じコマンドを `--found` なしで実行すればよい。

開発の決まりと品質ゲートは [CLAUDE.md](CLAUDE.md) を参照。
