// -*- mode: Bluespec; -*-

/// The BFT consensus of `oleaph-bft` (`crates/bft/src/core.rs`): `HotStuff`
/// with a two-chain commit, as `HotStuff-2` and Jolteon run it, a leader for
/// each view, and timeouts that a quorum certifies.
///
/// The model is at the level of the protocol, not of the bytes:
///
/// - Signatures are unforgeable, so a message names its signer, and only
///   the faulty replicas sign what the protocol does not say.
/// - The network is a pool of every message ever sent. Any replica may read
///   any of them, at any time, in any order, or never: delays, reordering,
///   duplication and loss.
/// - Every block body is available to everyone (`Fetch` always succeeds
///   eventually); what a replica does with a block it lacks is a liveness
///   question, and the model is about safety.
/// - A certificate is any quorum of matching votes (or timeouts) in the
///   pool. A replica may learn of any certificate at any time: a faulty
///   replica can always forward one, in a `Timeout`'s high certificate or a
///   `Skip`, so this adds no behaviour the code does not have.
/// - Faults are counted by operator: one replica per operator, so the
///   faulty set is a set of replicas.
module bft {
  // ------------------------------------------------------------------
  // Parameters
  // ------------------------------------------------------------------

  /// The committee.
  const Replicas: Set[str]
  /// The replicas that do anything their keys can sign.
  const Faulty: Set[str]
  /// The last view the model runs.
  const MaxView: int
  /// How many blocks a faulty leader can propose in one of its views.
  const Slots: int
  /// The leader of each view, `1..MaxView`. The code draws it from a hash
  /// of the view; any fixed schedule is one of the schedules it can draw.
  const LeaderOf: int -> str

  pure val Correct: Set[str] = Replicas.exclude(Faulty)
  /// `f`: the most with `3f + 1 <= n`, as `committee::faults`.
  pure val F: int = (Replicas.size() - 1) / 3
  /// `n - f`, as `committee::quorum`.
  pure val Quorum: int = Replicas.size() - F
  pure val Views: Set[int] = 1.to(MaxView)

  // ------------------------------------------------------------------
  // Types
  // ------------------------------------------------------------------

  type View = int
  type BlockId = int

  /// A block, with what the model derives from its chain: its height and
  /// every block before it.
  type Block = {
    view: View,
    parent: BlockId,
    // The view of the certificate it extends, `Justify::view`. The
    // certificate certifies `parent`.
    justify: View,
    proposer: str,
    height: int,
    ancestors: Set[BlockId],
  }

  /// `Justify`: a certificate of `block` from `view`. The genesis block's
  /// is from view 0.
  type QC = { view: View, block: BlockId }

  type Vote = { voter: str, view: View, block: BlockId }

  /// `Timeout`: `voter` gives up on `view`, knowing `high`.
  type Timeout = { voter: str, view: View, high: QC }

  /// `TimeoutCertificate`: a quorum gave up on `view`; the newest
  /// certificate any of them knew is from `high`.
  type TC = { view: View, high: View }

  type MaybeTC = NoTC | Entered(TC)

  /// `Proposal`: a block and, when the view before timed out, the
  /// certificate of that. The leader signs the block alone, so anyone can
  /// pass the block on with another timeout certificate, or none.
  type Proposal = { block: BlockId, timeout: MaybeTC }

  /// What a correct replica keeps (`Core`).
  type Replica = {
    view: View,
    // The last view it voted or gave up in. Durable.
    voted: View,
    // The newest certificate it knows. Durable.
    lock: QC,
    // The last view it proposed in.
    proposed: View,
    // The certificate it entered its view with, if the view before timed
    // out.
    entered: MaybeTC,
    // Every block it decided. Durable.
    decided: Set[BlockId],
  }

  pure val Genesis: BlockId = 0
  pure val GenesisQC: QC = { view: 0, block: Genesis }
  pure val GenesisBlock: Block = {
    view: 0, parent: Genesis, justify: 0, proposer: "", height: 0, ancestors: Set(),
  }

  /// A block's id: its view and a slot. A correct leader proposes once,
  /// in slot 0; a faulty one may use them all.
  pure def blockId(view: View, slot: int): BlockId = view * Slots + slot

  pure def max(a: int, b: int): int = if (a > b) a else b

  // ------------------------------------------------------------------
  // State
  // ------------------------------------------------------------------

  var replicas: str -> Replica
  var blocks: BlockId -> Block
  var proposals: Set[Proposal]
  var votes: Set[Vote]
  var timeouts: Set[Timeout]
  /// Whether a correct leader ever proposed twice in one view.
  var equivocated: bool

  // ------------------------------------------------------------------
  // Certificates
  // ------------------------------------------------------------------

  /// Whether a quorum voted for `q.block` in `q.view`.
  def certified(q: QC): bool =
    q == GenesisQC
      or votes.filter(v => v.view == q.view and v.block == q.block)
              .map(v => v.voter)
              .size() >= Quorum

  /// Every certificate that the votes so far make.
  def qcs: Set[QC] =
    votes.map(v => { view: v.view, block: v.block }).filter(q => certified(q)).union(Set(GenesisQC))

  /// Whether a quorum's timeouts of `tc.view` make a certificate whose
  /// newest known certificate is from `tc.high`: one of them knew `high`,
  /// and with the others that knew no newer one they are a quorum.
  def timeoutCertified(tc: TC): bool = and {
    timeouts.exists(t => t.view == tc.view and t.high.view == tc.high),
    timeouts.filter(t => t.view == tc.view and t.high.view <= tc.high)
            .map(t => t.voter)
            .size() >= Quorum,
  }

  /// Every timeout certificate that the timeouts so far make.
  def tcs: Set[TC] =
    timeouts.map(t => { view: t.view, high: t.high.view }).filter(tc => timeoutCertified(tc))

  /// The model's `Proposal::verify`: the leader of the block's view
  /// proposed it, and the block extends a certificate from the view right
  /// before, or the view before timed out and the block extends a
  /// certificate at least as new as any signer of the timeout knew. Every
  /// certificate in the model is genuine, so it need not be checked.
  def valid(p: Proposal): bool = {
    val b = blocks.get(p.block)
    val afterTimeout = match p.timeout {
      | NoTC => false
      | Entered(tc) => tc.view + 1 == b.view and b.justify >= tc.high
    }
    and {
      LeaderOf.get(b.view) == b.proposer,
      b.view == b.justify + 1 or afterTimeout,
    }
  }

  // ------------------------------------------------------------------
  // A correct replica's steps, as pure functions of its state
  // ------------------------------------------------------------------

  /// `Core::enter`.
  pure def enter(s: Replica, view: View, entered: MaybeTC): Replica =
    { ...s, view: view, entered: entered }

  /// `Core::observe` with `find_commit` and `decide`: learns of `q`, which
  /// may be newer than the lock, end the view, or, when it certifies a
  /// block proposed in the view right after its own certificate's, decide
  /// that block's parent and everything before it.
  def observe(s: Replica, q: QC): Replica = {
    val child = blocks.get(q.block)
    val commits = q.block != Genesis and child.view == q.view and child.view == child.justify + 1
    val decided =
      if (commits)
        s.decided.union(blocks.get(child.parent).ancestors).union(Set(child.parent)).exclude(Set(Genesis))
      else s.decided
    val learned = { ...s, lock: if (q.view > s.lock.view) q else s.lock, decided: decided }
    if (q.view + 1 > s.view) enter(learned, q.view + 1, NoTC) else learned
  }

  /// `Core::timed_out`: leaves the view that `tc` ends.
  pure def timedOut(s: Replica, tc: TC): Replica =
    if (tc.view + 1 > s.view) enter(s, tc.view + 1, Entered(tc)) else s

  /// `Core::check_vote`.
  pure def mayVote(s: Replica, b: Block): bool = and {
    b.view == s.view,
    s.voted < b.view,
    b.justify >= s.lock.view,
  }

  // ------------------------------------------------------------------
  // Actions of the correct replicas
  // ------------------------------------------------------------------

  /// `Core::propose`: the leader of its view proposes once, on top of its
  /// lock, and votes for its own block in the same step, so that the
  /// journal holds the vote before the proposal leaves. The code proposes
  /// only when there is something to decide; the model may propose
  /// whenever the code may.
  action propose(r: str): bool = {
    val s = replicas.get(r)
    val id = blockId(s.view, 0)
    val parent = blocks.get(s.lock.block)
    val block = {
      view: s.view,
      parent: s.lock.block,
      justify: s.lock.view,
      proposer: r,
      height: parent.height + 1,
      ancestors: parent.ancestors.union(Set(s.lock.block)),
    }
    val proposed = { ...s, proposed: s.view }
    all {
      s.view <= MaxView,
      LeaderOf.get(s.view) == r,
      s.proposed < s.view,
      if (blocks.keys().contains(id)) all {
        // It proposed in this view before: the model has no slot for a
        // second block, and says so.
        equivocated' = true,
        blocks' = blocks,
        proposals' = proposals,
        replicas' = replicas.set(r, proposed),
        votes' = votes,
      } else all {
        equivocated' = equivocated,
        blocks' = blocks.put(id, block),
        proposals' = proposals.union(Set({ block: id, timeout: s.entered })),
        if (mayVote(proposed, block)) all {
          replicas' = replicas.set(r, { ...proposed, voted: s.view }),
          votes' = votes.union(Set({ voter: r, view: s.view, block: id })),
        } else all {
          replicas' = replicas.set(r, proposed),
          votes' = votes,
        },
      },
      timeouts' = timeouts,
    }
  }

  /// `Core::on_proposal`: learns the block's certificate and the timeout
  /// certificate with it, then votes if it may, for the next leader.
  action receiveProposal(r: str, p: Proposal): bool = {
    val b = blocks.get(p.block)
    val learned = observe(replicas.get(r), { view: b.justify, block: b.parent })
    val s = match p.timeout {
      | NoTC => learned
      | Entered(tc) => timedOut(learned, tc)
    }
    all {
      valid(p),
      mayVote(s, b) or s != replicas.get(r),
      if (mayVote(s, b)) all {
        replicas' = replicas.set(r, { ...s, voted: b.view }),
        votes' = votes.union(Set({ voter: r, view: b.view, block: p.block })),
      } else all {
        replicas' = replicas.set(r, s),
        votes' = votes,
      },
      blocks' = blocks,
      proposals' = proposals,
      timeouts' = timeouts,
      equivocated' = equivocated,
    }
  }

  /// Learns of a certificate: as the next leader collecting votes, or from
  /// a proposal, a timeout or a forwarded one.
  action observeQC(r: str, q: QC): bool = all {
    observe(replicas.get(r), q) != replicas.get(r),
    replicas' = replicas.set(r, observe(replicas.get(r), q)),
    blocks' = blocks,
    proposals' = proposals,
    votes' = votes,
    timeouts' = timeouts,
    equivocated' = equivocated,
  }

  /// `Core::expire`: gives up on its view, again each time the timer
  /// expires, with the newest certificate it knows then.
  action timeout(r: str): bool = {
    val s = replicas.get(r)
    val t = { voter: r, view: s.view, high: s.lock }
    all {
      s.view <= MaxView,
      not(timeouts.contains(t)),
      replicas' = replicas.set(r, { ...s, voted: max(s.voted, s.view) }),
      timeouts' = timeouts.union(Set(t)),
      equivocated' = equivocated,
      blocks' = blocks,
      proposals' = proposals,
      votes' = votes,
    }
  }

  /// `Core::count_timeout`: a quorum of timeouts of its own view, each of
  /// which it learned the high certificate of (`Core::on_timeout`), ends
  /// the view.
  action collectTimeouts(r: str, tc: TC): bool = {
    val s = replicas.get(r)
    // The newest certificate in the timeouts. A view certifies one block,
    // so every timeout that names its view names the same certificate.
    val top = timeouts.filter(t => t.view == tc.view and t.high.view == tc.high)
                      .fold(GenesisQC, (_, t) => t.high)
    all {
      tc.view == s.view,
      replicas' = replicas.set(r, timedOut(observe(s, top), tc)),
      equivocated' = equivocated,
      blocks' = blocks,
      proposals' = proposals,
      votes' = votes,
      timeouts' = timeouts,
    }
  }

  /// `Message::Skip`: a timeout certificate from another replica ends its
  /// view, without the timeouts in it.
  action receiveSkip(r: str, tc: TC): bool = all {
    timedOut(replicas.get(r), tc) != replicas.get(r),
    replicas' = replicas.set(r, timedOut(replicas.get(r), tc)),
    blocks' = blocks,
    proposals' = proposals,
    votes' = votes,
    timeouts' = timeouts,
    equivocated' = equivocated,
  }

  /// `Core::recover`: a crash forgets everything but what the journal
  /// keeps, and the replica starts again in the view after its lock's or
  /// the last it voted or gave up in, without proposing there.
  action crash(r: str): bool = {
    val s = replicas.get(r)
    val restarted = {
      ...s,
      view: max(s.lock.view + 1, s.voted),
      proposed: s.voted,
      entered: NoTC,
    }
    all {
      restarted != s,
      replicas' = replicas.set(r, restarted),
      blocks' = blocks,
      proposals' = proposals,
      votes' = votes,
      timeouts' = timeouts,
      equivocated' = equivocated,
    }
  }

  // ------------------------------------------------------------------
  // Actions of the faulty replicas: anything their keys can sign
  // ------------------------------------------------------------------

  /// A vote for any block. A vote whose view is not its block's never
  /// meets enough correct votes to matter, so the model leaves it out.
  action faultyVote(f: str, b: BlockId): bool = all {
    b != Genesis,
    votes' = votes.union(Set({ voter: f, view: blocks.get(b).view, block: b })),
    replicas' = replicas,
    blocks' = blocks,
    proposals' = proposals,
    timeouts' = timeouts,
    equivocated' = equivocated,
  }

  /// A timeout of any view, with any genuine certificate.
  action faultyTimeout(f: str, v: View, q: QC): bool = all {
    timeouts' = timeouts.union(Set({ voter: f, view: v, high: q })),
    equivocated' = equivocated,
    replicas' = replicas,
    blocks' = blocks,
    proposals' = proposals,
    votes' = votes,
  }

  /// A faulty leader proposes any number of blocks in its view, each on
  /// any genuine certificate.
  action faultyPropose(f: str, v: View, slot: int, q: QC): bool = {
    val id = blockId(v, slot)
    val parent = blocks.get(q.block)
    all {
      LeaderOf.get(v) == f,
      q.view < v,
      not(blocks.keys().contains(id)),
      blocks' = blocks.put(id, {
        view: v,
        parent: q.block,
        justify: q.view,
        proposer: f,
        height: parent.height + 1,
        ancestors: parent.ancestors.union(Set(q.block)),
      }),
      proposals' = proposals.union(Set({ block: id, timeout: NoTC })),
      replicas' = replicas,
      votes' = votes,
      timeouts' = timeouts,
      equivocated' = equivocated,
    }
  }

  /// Any block passed on with any genuine timeout certificate: the leader's
  /// signature does not cover it.
  action relay(b: BlockId, tc: TC): bool = all {
    b != Genesis,
    not(proposals.contains({ block: b, timeout: Entered(tc) })),
    proposals' = proposals.union(Set({ block: b, timeout: Entered(tc) })),
    replicas' = replicas,
    blocks' = blocks,
    votes' = votes,
    timeouts' = timeouts,
    equivocated' = equivocated,
  }

  // ------------------------------------------------------------------
  // The system
  // ------------------------------------------------------------------

  action init: bool = all {
    replicas' = Correct.mapBy(_ => {
      view: 1, voted: 0, lock: GenesisQC, proposed: 0, entered: NoTC, decided: Set(),
    }),
    blocks' = Map(Genesis -> GenesisBlock),
    proposals' = Set(),
    votes' = Set(),
    timeouts' = Set(),
    equivocated' = false,
  }

  action correctStep: bool = {
    nondet r = Correct.oneOf()
    any {
      propose(r),
      all { proposals != Set(), nondet p = proposals.oneOf() receiveProposal(r, p) },
      nondet q = qcs.oneOf() observeQC(r, q),
      timeout(r),
      all { tcs != Set(), nondet tc = tcs.oneOf() collectTimeouts(r, tc) },
      all { tcs != Set(), nondet tc = tcs.oneOf() receiveSkip(r, tc) },
      crash(r),
    }
  }

  action faultyStep: bool = all {
    Faulty != Set(),
    nondet f = Faulty.oneOf()
    nondet v = Views.oneOf()
    nondet q = qcs.oneOf()
    nondet b = blocks.keys().oneOf()
    nondet slot = 0.to(Slots - 1).oneOf()
    any {
      faultyVote(f, b),
      faultyTimeout(f, v, q),
      faultyPropose(f, v, slot, q),
      all { tcs != Set(), nondet tc = tcs.oneOf() relay(b, tc) },
    },
  }

  action step: bool = any { correctStep, faultyStep }

  // ------------------------------------------------------------------
  // Safety
  // ------------------------------------------------------------------

  /// Whether one of `a` and `b` is the other or comes before it.
  def onOneChain(a: BlockId, b: BlockId): bool =
    a == b or blocks.get(a).ancestors.contains(b) or blocks.get(b).ancestors.contains(a)

  def decidedByCorrect: Set[BlockId] = Correct.map(r => replicas.get(r).decided).flatten()

  /// Agreement: every block a correct replica decides is on one chain.
  val agreement: bool =
    tuples(decidedByCorrect, decidedByCorrect).forall(((a, b)) => onOneChain(a, b))

  /// No two different blocks are decided at the same height.
  val oneBlockPerHeight: bool =
    tuples(decidedByCorrect, decidedByCorrect).forall(((a, b)) =>
      blocks.get(a).height == blocks.get(b).height implies a == b
    )

  /// A correct replica votes once in a view at most.
  val voteOnce: bool =
    votes.forall(a => votes.forall(b =>
      (Correct.contains(a.voter) and a.voter == b.voter and a.view == b.view) implies a == b
    ))

  /// A correct replica never votes in a view after giving up on it, nor in
  /// a view after it.
  val noVoteAfterTimeout: bool =
    votes.forall(v => Correct.contains(v.voter) implies v.view <= replicas.get(v.voter).voted)

  /// A view certifies one block at most: two quorums share a correct
  /// replica, which votes once.
  val oneCertificatePerView: bool =
    qcs.forall(a => qcs.forall(b => a.view == b.view implies a.block == b.block))

  /// A correct replica's lock is a genuine certificate, and its view is
  /// past the lock's.
  val lockIsCertified: bool =
    Correct.forall(r => certified(replicas.get(r).lock) and replicas.get(r).view > replicas.get(r).lock.view)

  val safety: bool = and {
    agreement,
    oneBlockPerHeight,
    voteOnce,
    noVoteAfterTimeout,
    oneCertificatePerView,
    lockIsCertified,
    not(equivocated),
  }

  // ------------------------------------------------------------------
  // States the scenarios reach, to show the invariants above do not
  // hold for want of anything happening.
  // ------------------------------------------------------------------

  /// A correct replica decided a block.
  val decidedSomething: bool = decidedByCorrect != Set()

  /// A correct replica decided a block proposed after a timeout.
  val decidedAfterTimeout: bool =
    decidedByCorrect.exists(b => blocks.get(b).justify + 1 < blocks.get(b).view)

  /// Two certified blocks on different chains: a faulty leader forked and
  /// both forks gathered a quorum, which the lock must keep from both
  /// being decided.
  val certifiedFork: bool =
    qcs.exists(a => qcs.exists(b => not(onOneChain(a.block, b.block))))

  /// A correct leader proposed a block that the others reject as
  /// unjustified.
  val unjustifiedProposal: bool =
    proposals.exists(p => Correct.contains(blocks.get(p.block).proposer) and not(valid(p)))
}

/// Four replicas, one of them faulty and leading the second view.
module n4f1 {
  import bft(
    Replicas = Set("n1", "n2", "n3", "n4"),
    Faulty = Set("n4"),
    MaxView = 5,
    Slots = 2,
    LeaderOf = Map(1 -> "n1", 2 -> "n4", 3 -> "n2", 4 -> "n3", 5 -> "n4"),
  ).*

  pure val GaveUp1: TC = { view: 1, high: 0 }
  pure val GaveUp2: TC = { view: 2, high: 0 }

  /// The faulty n4 proposes two blocks in view 2 and votes for both; n1
  /// votes for one, n2 for the other. One faulty vote and one correct one
  /// are not a quorum, so neither block is certified.
  run oneFaultCannotCertifyAForkTest =
    init
      .then(propose("n1"))
      .then(receiveProposal("n2", { block: 2, timeout: NoTC }))
      .then(receiveProposal("n3", { block: 2, timeout: NoTC }))
      .then(faultyPropose("n4", 2, 0, { view: 1, block: 2 }))
      .then(faultyPropose("n4", 2, 1, { view: 1, block: 2 }))
      .then(receiveProposal("n1", { block: 4, timeout: NoTC }))
      .then(receiveProposal("n2", { block: 5, timeout: NoTC }))
      .then(faultyVote("n4", 4))
      .then(faultyVote("n4", 5))
      .expect(not(certified({ view: 2, block: 4 })) and not(certified({ view: 2, block: 5 })))

  /// A replica that voted and crashed does not vote again in that view.
  run crashKeepsTheVoteTest =
    init
      .then(propose("n1"))
      .then(receiveProposal("n2", { block: 2, timeout: NoTC }))
      .then(crash("n2"))
      .then(receiveProposal("n2", { block: 2, timeout: NoTC }))
      .fail()

  /// The faulty n4 leads view 2 and keeps the certificate of view 1's
  /// block to itself. The others give up on views 1 and 2 knowing only
  /// genesis, and view 3 certifies a block beside the one view 1 did. That
  /// block, not view 1's, is decided once view 4 certifies its child.
  run forkAfterTimeoutsTest =
    init
      .then(propose("n1"))
      .then(receiveProposal("n2", { block: 2, timeout: NoTC }))
      .then(receiveProposal("n3", { block: 2, timeout: NoTC }))
      .expect(qcs.contains({ view: 1, block: 2 }))
      .then(timeout("n1"))
      .then(timeout("n2"))
      .then(timeout("n3"))
      .then(collectTimeouts("n1", GaveUp1))
      .then(collectTimeouts("n2", GaveUp1))
      .then(collectTimeouts("n3", GaveUp1))
      .then(timeout("n1"))
      .then(timeout("n2"))
      .then(timeout("n3"))
      .then(collectTimeouts("n1", GaveUp2))
      .then(collectTimeouts("n2", GaveUp2))
      .then(collectTimeouts("n3", GaveUp2))
      .expect(Correct.forall(r => replicas.get(r).view == 3))
      .then(propose("n2"))
      .then(receiveProposal("n1", { block: 6, timeout: Entered(GaveUp2) }))
      .then(receiveProposal("n3", { block: 6, timeout: Entered(GaveUp2) }))
      .expect(certifiedFork and not(decidedSomething))
      .then(observeQC("n3", { view: 3, block: 6 }))
      .then(propose("n3"))
      .then(receiveProposal("n1", { block: 8, timeout: NoTC }))
      .then(receiveProposal("n2", { block: 8, timeout: NoTC }))
      .then(observeQC("n1", { view: 4, block: 8 }))
      .expect(replicas.get("n1").decided == Set(6) and decidedAfterTimeout and safety)
}

/// Four replicas, all correct.
module n4f0 {
  import bft(
    Replicas = Set("n1", "n2", "n3", "n4"),
    Faulty = Set(),
    MaxView = 5,
    Slots = 1,
    LeaderOf = Map(1 -> "n1", 2 -> "n2", 3 -> "n3", 4 -> "n4", 5 -> "n1"),
  ).*

  /// With one slot, the block of view `v` is block `v`.
  pure val Fresh: MaybeTC = NoTC

  /// Two views, each certified: the second certifies the first's child,
  /// which commits the first block, and nothing else.
  run twoChainCommitsTest =
    init
      .then(propose("n1"))
      .then(receiveProposal("n2", { block: 1, timeout: Fresh }))
      .then(receiveProposal("n3", { block: 1, timeout: Fresh }))
      // n2 leads view 2 and collects the votes of n1, n2 and n3.
      .then(observeQC("n2", { view: 1, block: 1 }))
      .expect(replicas.get("n2").view == 2 and replicas.get("n2").decided == Set())
      .then(propose("n2"))
      .then(receiveProposal("n3", { block: 2, timeout: Fresh }))
      .then(receiveProposal("n4", { block: 2, timeout: Fresh }))
      .then(observeQC("n3", { view: 2, block: 2 }))
      .expect(replicas.get("n3").decided == Set(1) and safety)

  /// Every replica gives up on view 1; the leader of view 2 collects the
  /// timeouts and proposes with their certificate, which the others accept.
  run timeoutStartsTheNextViewTest =
    init
      .then(timeout("n1"))
      .then(timeout("n2"))
      .then(timeout("n3"))
      .then(collectTimeouts("n2", { view: 1, high: 0 }))
      .expect(replicas.get("n2").view == 2)
      .then(propose("n2"))
      .expect(proposals.forall(p => valid(p)))
      .then(receiveProposal("n4", { block: 2, timeout: Entered({ view: 1, high: 0 }) }))
      .expect(votes.contains({ voter: "n4", view: 2, block: 2 }))

  /// n3 knows view 2's certificate; n1, n2 and n4 know only view 1's, give
  /// up on views 2 and 3, and n4 proposes in view 4 on view 1's
  /// certificate, as their timeout certificate allows. n3's lock keeps it
  /// from voting for the older branch.
  run lockRefusesAnOlderBranchTest =
    init
      .then(propose("n1"))
      .then(receiveProposal("n2", { block: 1, timeout: Fresh }))
      .then(receiveProposal("n3", { block: 1, timeout: Fresh }))
      .then(observeQC("n2", { view: 1, block: 1 }))
      .then(propose("n2"))
      .then(receiveProposal("n3", { block: 2, timeout: Fresh }))
      .then(receiveProposal("n4", { block: 2, timeout: Fresh }))
      .then(observeQC("n3", { view: 2, block: 2 }))
      .then(observeQC("n1", { view: 1, block: 1 }))
      .then(timeout("n1"))
      .then(timeout("n2"))
      .then(timeout("n4"))
      .then(collectTimeouts("n1", { view: 2, high: 1 }))
      .then(collectTimeouts("n2", { view: 2, high: 1 }))
      .then(collectTimeouts("n4", { view: 2, high: 1 }))
      .then(timeout("n1"))
      .then(timeout("n2"))
      .then(timeout("n4"))
      .then(collectTimeouts("n4", { view: 3, high: 1 }))
      .then(propose("n4"))
      .expect(proposals.contains({ block: 4, timeout: Entered({ view: 3, high: 1 }) }))
      .then(receiveProposal("n3", { block: 4, timeout: Entered({ view: 3, high: 1 }) }))
      .expect(replicas.get("n3").view == 4 and not(votes.exists(v => v.voter == "n3" and v.view == 4)))

  /// n4 misses everything of views 1 to 3 but a `Skip` of view 3, whose
  /// timeouts knew view 1's certificate. It leads view 4 and proposes on
  /// the newest certificate it knows, genesis, which the timeout
  /// certificate does not justify: everyone rejects the block and view 4
  /// times out. Safe, but a view lost: the code's `Skip` carries the
  /// timeouts' views and not their certificates, so n4 cannot catch up.
  run skippedLeaderProposesUnjustifiedTest =
    init
      .then(propose("n1"))
      .then(receiveProposal("n2", { block: 1, timeout: Fresh }))
      .then(receiveProposal("n3", { block: 1, timeout: Fresh }))
      .then(observeQC("n1", { view: 1, block: 1 }))
      .then(observeQC("n2", { view: 1, block: 1 }))
      .then(observeQC("n3", { view: 1, block: 1 }))
      .then(timeout("n1"))
      .then(timeout("n2"))
      .then(timeout("n3"))
      .then(collectTimeouts("n1", { view: 2, high: 1 }))
      .then(collectTimeouts("n2", { view: 2, high: 1 }))
      .then(collectTimeouts("n3", { view: 2, high: 1 }))
      .then(timeout("n1"))
      .then(timeout("n2"))
      .then(timeout("n3"))
      .then(receiveSkip("n4", { view: 3, high: 1 }))
      .then(propose("n4"))
      .expect(unjustifiedProposal)
      .then(receiveProposal("n1", { block: 4, timeout: Entered({ view: 3, high: 1 }) }))
      .fail()

  /// A replica that gave up on a view does not vote in it.
  run noVoteAfterGivingUpTest =
    init
      .then(propose("n1"))
      .then(timeout("n2"))
      .then(receiveProposal("n2", { block: 1, timeout: Fresh }))
      .fail()
}

/// Four replicas, two of them faulty: more than the protocol tolerates.
/// Agreement must fail here, or the model could not see it fail at all.
module n4f2 {
  import bft(
    Replicas = Set("n1", "n2", "n3", "n4"),
    Faulty = Set("n3", "n4"),
    MaxView = 4,
    Slots = 2,
    LeaderOf = Map(1 -> "n1", 2 -> "n4", 3 -> "n3", 4 -> "n2"),
  ).*

  /// The faulty leaders of views 2 and 3 fork the chain, and with two
  /// faulty voters each branch gathers a quorum: n1 decides one block of
  /// view 2, n2 the other.
  run forkDecidedTest =
    init
      .then(propose("n1"))
      .then(receiveProposal("n2", { block: 2, timeout: NoTC }))
      .then(faultyVote("n3", 2))
      .then(faultyPropose("n4", 2, 0, { view: 1, block: 2 }))
      .then(faultyPropose("n4", 2, 1, { view: 1, block: 2 }))
      .then(receiveProposal("n1", { block: 4, timeout: NoTC }))
      .then(receiveProposal("n2", { block: 5, timeout: NoTC }))
      .then(faultyVote("n3", 4))
      .then(faultyVote("n4", 4))
      .then(faultyVote("n3", 5))
      .then(faultyVote("n4", 5))
      .then(faultyPropose("n3", 3, 0, { view: 2, block: 4 }))
      .then(faultyPropose("n3", 3, 1, { view: 2, block: 5 }))
      .then(receiveProposal("n1", { block: 6, timeout: NoTC }))
      .then(receiveProposal("n2", { block: 7, timeout: NoTC }))
      .then(faultyVote("n3", 6))
      .then(faultyVote("n4", 6))
      .then(faultyVote("n3", 7))
      .then(faultyVote("n4", 7))
      .then(observeQC("n1", { view: 3, block: 6 }))
      .then(observeQC("n2", { view: 3, block: 7 }))
      .expect(replicas.get("n1").decided == Set(2, 4))
      .expect(replicas.get("n2").decided == Set(2, 5))
      .expect(not(agreement) and not(oneBlockPerHeight))
}
