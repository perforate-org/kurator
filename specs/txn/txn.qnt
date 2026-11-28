// -*- mode: Bluespec; -*-

/// Transactions across shards: the two-phase commit of
/// `crates/storage/src/two_phase.rs` (what a shard does with `Prepare` and
/// `Decide`, `KvState::apply`) driven by the coordinator of
/// `crates/client/src/across.rs` (`Across::commit`) and resolved by
/// `crates/client/src/resolve.rs`, together with transactions on one
/// shard that only read (`KvState::confirm`, used by `Txn::commit` in
/// `crates/raft/src/replica.rs` and `crates/bft/src/replica.rs`).
///
/// The model is at the level of the protocol:
///
/// - A shard is one replicated state: its log orders every command it
///   applies, and the model applies each at once. Replication, leaders and
///   crashes of replicas are the consensus specs' business.
/// - A transaction's keys, what it reads and writes, are fixed up front; a
///   value is the transaction that wrote it, so a read names its writer.
/// - A coordinator may stop after any step, forever: nothing makes it go
///   on. Anyone may then tell a shard how the primary decided, and anyone
///   may resolve a lock, at any time: the model has no clock, so every
///   deadline may have passed (a resolve that comes early only aborts).
/// - Commit points are recorded on a ghost: `serial` holds each key's
///   value in the order the transactions committed, and a transaction's
///   reads must equal it at its commit point.
module txn {
  // ------------------------------------------------------------------
  // Parameters
  // ------------------------------------------------------------------

  /// The shard each key lives on.
  const ShardOf: str -> str
  /// What each transaction reads and writes.
  const Reads: str -> Set[str]
  const Writes: str -> Set[str]
  /// The shard each transaction across shards decides on.
  const Primary: str -> str
  /// Transactions run by a coordinator in two phases.
  const Across: Set[str]
  /// Transactions on one shard that only read: they log nothing.
  const Local: Set[str]

  pure val Keys: Set[str] = ShardOf.keys()
  pure val Shards: Set[str] = Keys.map(k => ShardOf.get(k))
  pure val Txns: Set[str] = Across.union(Local)
  /// The value of a key nobody wrote.
  pure val Initial: str = "init"
  /// A key not read yet.
  pure val Unread: str = ""
  /// A shard not begun yet.
  pure val NotBegun: int = -1

  pure def shardsOf(t: str): Set[str] = Reads.get(t).union(Writes.get(t)).map(k => ShardOf.get(k))
  pure def on(ks: Set[str], s: str): Set[str] = ks.filter(k => ShardOf.get(k) == s)

  // ------------------------------------------------------------------
  // Types
  // ------------------------------------------------------------------

  type Decision = Undecided | Commit | Abort

  /// What a shard holds about transactions across shards (`Locks`).
  type ShardState = {
    version: int,
    // The parts it prepared and has not been told about.
    locks: Set[str],
    // How it decided each transaction it was told about.
    decided: str -> Decision,
  }

  /// A write of a key: at which version of its shard, by whom.
  type Write = { at: int, by: str }

  /// How far a transaction got, as its client sees it.
  type Phase = Running | Refusing | Committed | Aborted

  type Coordinator = {
    // The version each shard read at, `NotBegun` until it begins there.
    readAt: str -> int,
    // What each key read, `Unread` until it is.
    seen: str -> str,
    // The shards that locked its part.
    prepared: Set[str],
    phase: Phase,
  }

  type Ghost = {
    // Each key's value, the transactions applied in commit order.
    serial: str -> str,
    // The committed transactions whose reads were not current at their
    // commit point.
    stale: Set[str],
  }

  // ------------------------------------------------------------------
  // State
  // ------------------------------------------------------------------

  var shards: str -> ShardState
  var kv: str -> List[Write]
  var coords: str -> Coordinator
  var ghost: Ghost

  // ------------------------------------------------------------------
  // A shard's state, read
  // ------------------------------------------------------------------

  /// The value of `k` at version `v` of its shard (`View::get`).
  def valueAt(k: str, v: int): str =
    kv.get(k).foldl(Initial, (value, w) => if (w.at <= v) w.by else value)

  /// The version of the newest write of `k`.
  def newest(k: str): int = kv.get(k).foldl(0, (_, w) => w.at)

  /// `KvState::check` on shard `s` for `t`'s part there: something it read
  /// was written after its read version, or another prepared transaction
  /// holds a key it touches (`Locks::conflict`).
  def conflict(s: str, t: str): bool = {
    val reads = on(Reads.get(t), s)
    val writes = on(Writes.get(t), s)
    val changed = reads.exists(k => newest(k) > coords.get(t).readAt.get(s))
    val locked = shards.get(s).locks.exists(u => u != t and or {
      writes.exists(k => Reads.get(u).contains(k) or Writes.get(u).contains(k)),
      reads.exists(k => Writes.get(u).contains(k)),
    })
    changed or locked
  }

  /// Whether every key `t` reads was read.
  def readAll(t: str): bool = Reads.get(t).forall(k => coords.get(t).seen.get(k) != Unread)

  // ------------------------------------------------------------------
  // A shard's steps
  // ------------------------------------------------------------------

  /// What `decideIn` leaves: the shard, the keys, and how `t` came out.
  type Decided = { shard: ShardState, store: str -> List[Write], result: Decision }

  /// `KvState::decide` on shard `s`, which holds `held`, with keys `store`:
  /// what it holds after being told `d` for `t`, and how `t` came out
  /// there. A decision is made once; a commit of a part it does not hold
  /// aborts.
  pure def decideIn(held: ShardState, store: str -> List[Write], s: str, t: str, d: Decision): Decided = {
    val known = held.decided.get(t)
    if (known != Undecided) { shard: held, store: store, result: known }
    else {
      val v = held.version + 1
      val commits = d == Commit and held.locks.contains(t)
      val result = if (commits) Commit else Abort
      val written = on(Writes.get(t), s)
      {
        shard: { version: v, locks: held.locks.exclude(Set(t)), decided: held.decided.set(t, result) },
        store: if (commits) store.keys().mapBy(k => if (written.contains(k)) store.get(k).append({ at: v, by: t }) else store.get(k)) else store,
        result: result,
      }
    }
  }

  /// `decideIn` on shard `s` as it is now.
  def decideOn(s: str, t: str, d: Decision): Decided = decideIn(shards.get(s), kv, s, t, d)

  /// The ghost after `t` commits: its reads checked against the serial
  /// order, its writes applied to it.
  def commitPoint(t: str): Ghost = {
    val current = Reads.get(t).forall(k => coords.get(t).seen.get(k) == ghost.serial.get(k))
    {
      serial: ghost.serial.keys().mapBy(k => if (Writes.get(t).contains(k)) t else ghost.serial.get(k)),
      stale: if (current) ghost.stale else ghost.stale.union(Set(t)),
    }
  }

  // ------------------------------------------------------------------
  // Actions
  // ------------------------------------------------------------------

  action init = all {
    shards' = Shards.mapBy(_ => { version: 0, locks: Set(), decided: Txns.mapBy(_ => Undecided) }),
    kv' = Keys.mapBy(_ => List()),
    coords' = Txns.mapBy(_ => {
      readAt: Shards.mapBy(_ => NotBegun),
      seen: Keys.mapBy(_ => Unread),
      prepared: Set(),
      phase: Running,
    }),
    ghost' = { serial: Keys.mapBy(_ => Initial), stale: Set() },
  }

  /// `Call::Begin`: `t` takes the version `s` applied last.
  action begin(t: str, s: str): bool = all {
    shardsOf(t).contains(s),
    coords.get(t).phase == Running,
    coords.get(t).readAt.get(s) == NotBegun,
    coords' = coords.setBy(t, c => { ...c, readAt: c.readAt.set(s, shards.get(s).version) }),
    shards' = shards, kv' = kv, ghost' = ghost,
  }

  /// `Call::Get`: `t` reads `k` at its read version of `k`'s shard.
  action read(t: str, k: str): bool = {
    val at = coords.get(t).readAt.get(ShardOf.get(k))
    all {
      Reads.get(t).contains(k),
      coords.get(t).phase == Running,
      at != NotBegun,
      coords.get(t).seen.get(k) == Unread,
      coords' = coords.setBy(t, c => { ...c, seen: c.seen.set(k, valueAt(k, at)) }),
      shards' = shards, kv' = kv, ghost' = ghost,
    }
  }

  /// `Command::Prepare` of `t`'s part on `s`, from its coordinator. A
  /// shard that decided `t` already, or finds a conflict, locks nothing,
  /// and the coordinator will abort.
  action prepare(t: str, s: str): bool = {
    val held = shards.get(s)
    val refuses = held.decided.get(t) != Undecided or conflict(s, t)
    all {
      Across.contains(t),
      shardsOf(t).contains(s),
      coords.get(t).phase == Running,
      coords.get(t).readAt.get(s) != NotBegun,
      readAll(t),
      not(coords.get(t).prepared.contains(s)),
      if (refuses) all {
        coords' = coords.setBy(t, c => { ...c, phase: Refusing }),
        shards' = shards,
      } else all {
        coords' = coords.setBy(t, c => { ...c, prepared: c.prepared.union(Set(s)) }),
        shards' = shards.set(s, { ...held, version: held.version + 1, locks: held.locks.union(Set(t)) }),
      },
      kv' = kv, ghost' = ghost,
    }
  }

  /// The coordinator decides `t` on its primary: commit once every shard
  /// locked its part, abort once one refused. The primary's decision is
  /// the commit point, and the client learns it.
  action decidePrimary(t: str): bool = {
    val c = coords.get(t)
    val p = Primary.get(t)
    val proposed = if (c.phase == Refusing) Abort else Commit
    val decided = decideOn(p, t, proposed)
    val fresh = shards.get(p).decided.get(t) == Undecided
    all {
      Across.contains(t),
      c.phase == Refusing or (c.phase == Running and c.prepared == shardsOf(t)),
      shards' = shards.set(p, decided.shard),
      kv' = decided.store,
      coords' = coords.set(t, { ...c, phase: if (decided.result == Commit) Committed else Aborted }),
      ghost' = if (fresh and decided.result == Commit) commitPoint(t) else ghost,
    }
  }

  /// Someone, the coordinator or a resolver, tells `s` how the primary
  /// decided `t`.
  action tell(t: str, s: str): bool = {
    val d = shards.get(Primary.get(t)).decided.get(t)
    val decided = decideOn(s, t, d)
    all {
      Across.contains(t),
      shardsOf(t).contains(s),
      s != Primary.get(t),
      d != Undecided,
      shards' = shards.set(s, decided.shard),
      kv' = decided.store,
      coords' = coords, ghost' = ghost,
    }
  }

  /// `Lock::resolve`: whoever runs into `t`'s lock on `s` asks the primary,
  /// which aborts `t` when it has not decided it, and decides `s` the
  /// way the primary did. The lock may be on the primary itself.
  action resolve(t: str, s: str): bool = {
    val p = Primary.get(t)
    val atPrimary = decideOn(p, t, Abort)
    val here = decideIn(shards.get(s), atPrimary.store, s, t, atPrimary.result)
    all {
      Across.contains(t),
      shards.get(s).locks.contains(t),
      if (s == p) all {
        shards' = shards.set(p, atPrimary.shard),
        kv' = atPrimary.store,
      } else all {
        shards' = shards.set(p, atPrimary.shard).set(s, here.shard),
        kv' = here.store,
      },
      coords' = coords, ghost' = ghost,
    }
  }

  /// `KvState::confirm`: a transaction on one shard that only read commits,
  /// with no log entry, unless what it read changed since or a prepared
  /// transaction holds a key it read.
  action confirm(t: str): bool = {
    val c = coords.get(t)
    val s = ShardOf.get(Reads.get(t).fold("", (_, k) => k))
    val confirmed = not(conflict(s, t))
    all {
      Local.contains(t),
      c.phase == Running,
      c.readAt.get(s) != NotBegun,
      readAll(t),
      coords' = coords.set(t, { ...c, phase: if (confirmed) Committed else Aborted }),
      ghost' = if (confirmed) commitPoint(t) else ghost,
      shards' = shards, kv' = kv,
    }
  }

  action step = any {
    nondet t = oneOf(Txns)
    nondet s = oneOf(Shards)
    nondet k = oneOf(Keys)
    any {
      begin(t, s),
      read(t, k),
      prepare(t, s),
      decidePrimary(t),
      tell(t, s),
      resolve(t, s),
      confirm(t),
    },
  }

  // ------------------------------------------------------------------
  // Properties
  // ------------------------------------------------------------------

  /// Every committed transaction read, on every shard, what the serial
  /// order of commit points says it should: committed transactions are
  /// serializable, each at its commit point.
  val serializable: bool = ghost.stale == Set()

  /// No shard commits a transaction that another aborts.
  val atomic: bool = Txns.forall(t => not(and {
    Shards.exists(a => shards.get(a).decided.get(t) == Commit),
    Shards.exists(b => shards.get(b).decided.get(t) == Abort),
  }))

  /// A shard commits only what the primary committed, and a client hears
  /// of a commit only once the primary made it.
  val primaryDecides: bool = Across.forall(t => and {
    Shards.forall(s => shards.get(s).decided.get(t) == Commit implies shards.get(Primary.get(t)).decided.get(t) == Commit),
    coords.get(t).phase == Committed implies shards.get(Primary.get(t)).decided.get(t) == Commit,
  })

  val safety: bool = serializable and atomic and primaryDecides
}

/// Two shards: `x` on A, `y` and `z` on B.
///
/// - T1 writes `x` and `z`, deciding on A.
/// - T2 reads `x` and writes `y`, deciding on B.
/// - T3 and T4 both read `x` and write it, deciding on A.
/// - R reads `y` and `z` on B alone, and writes nothing.
module two {
  import txn(
    ShardOf = Map("x" -> "A", "y" -> "B", "z" -> "B"),
    Reads = Map("T1" -> Set(), "T2" -> Set("x"), "T3" -> Set("x"), "T4" -> Set("x"), "R" -> Set("y", "z")),
    Writes = Map("T1" -> Set("x", "z"), "T2" -> Set("y"), "T3" -> Set("x"), "T4" -> Set("x"), "R" -> Set()),
    Primary = Map("T1" -> "A", "T2" -> "B", "T3" -> "A", "T4" -> "A", "R" -> "B"),
    Across = Set("T1", "T2", "T3", "T4"),
    Local = Set("R"),
  ).*

  /// T1 prepares on both shards and commits on A, and its coordinator
  /// stops there. T2 reads T1's `x` and commits `y` on B, where T1's part
  /// is still locked. R then reads `y` and `z` on B: it would see T2's
  /// write but not T1's, so it must not commit.
  run strandedCommitTest =
    init
      .then(begin("T1", "A"))
      .then(begin("T1", "B"))
      .then(prepare("T1", "A"))
      .then(prepare("T1", "B"))
      .then(decidePrimary("T1"))
      .then(begin("T2", "A"))
      .then(read("T2", "x"))
      .then(begin("T2", "B"))
      .then(prepare("T2", "A"))
      .then(prepare("T2", "B"))
      .then(decidePrimary("T2"))
      .expect(coords.get("T2").phase == Committed and coords.get("T2").seen.get("x") == "T1")
      .then(begin("R", "B"))
      .then(read("R", "y"))
      .then(read("R", "z"))
      .expect(coords.get("R").seen == Map("x" -> "", "y" -> "T2", "z" -> "init"))
      .then(confirm("R"))
      .expect(coords.get("R").phase == Aborted and safety)

  /// Once someone tells B, the same read commits and sees both.
  run strandedThenToldTest =
    init
      .then(begin("T1", "A"))
      .then(begin("T1", "B"))
      .then(prepare("T1", "A"))
      .then(prepare("T1", "B"))
      .then(decidePrimary("T1"))
      .then(tell("T1", "B"))
      .then(begin("R", "B"))
      .then(read("R", "y"))
      .then(read("R", "z"))
      .then(confirm("R"))
      .expect(coords.get("R").phase == Committed and coords.get("R").seen.get("z") == "T1" and safety)

  /// T4 read `x` before T3 committed a new one: its prepare finds the
  /// read changed, and it aborts.
  run lostUpdateTest =
    init
      .then(begin("T3", "A"))
      .then(begin("T4", "A"))
      .then(read("T3", "x"))
      .then(read("T4", "x"))
      .then(prepare("T3", "A"))
      .then(decidePrimary("T3"))
      .then(prepare("T4", "A"))
      .then(decidePrimary("T4"))
      .expect(coords.get("T3").phase == Committed and coords.get("T4").phase == Aborted and safety)

  /// T3 and T4 prepare at once: T3's lock refuses T4.
  run lockRefusesTest =
    init
      .then(begin("T3", "A"))
      .then(begin("T4", "A"))
      .then(read("T3", "x"))
      .then(read("T4", "x"))
      .then(prepare("T3", "A"))
      .then(prepare("T4", "A"))
      .then(decidePrimary("T3"))
      .then(decidePrimary("T4"))
      .expect(coords.get("T3").phase == Committed and coords.get("T4").phase == Aborted and safety)

  /// T1's coordinator stops after the commit point; whoever runs into its
  /// lock on B commits the part there, as the primary did.
  run resolvedAfterCommitTest =
    init
      .then(begin("T1", "A"))
      .then(begin("T1", "B"))
      .then(prepare("T1", "A"))
      .then(prepare("T1", "B"))
      .then(decidePrimary("T1"))
      .then(resolve("T1", "B"))
      .expect(shards.get("B").decided.get("T1") == Commit and kv.get("z") == List({ at: 2, by: "T1" }) and safety)

  /// T1's coordinator stops before the commit point; a resolver aborts
  /// it at the primary, and the coordinator, if it comes back, learns so.
  run resolvedBeforeCommitTest =
    init
      .then(begin("T1", "A"))
      .then(begin("T1", "B"))
      .then(prepare("T1", "A"))
      .then(prepare("T1", "B"))
      .then(resolve("T1", "B"))
      .then(decidePrimary("T1"))
      .expect(coords.get("T1").phase == Aborted and Shards.forall(s => shards.get(s).decided.get("T1") == Abort) and safety)

  /// The primary does not commit before every shard locked its part.
  run noCommitBeforeEveryPrepareTest =
    init
      .then(begin("T1", "A"))
      .then(begin("T1", "B"))
      .then(prepare("T1", "A"))
      .then(decidePrimary("T1"))
      .fail()
}
