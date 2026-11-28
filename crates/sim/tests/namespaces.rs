//! Namespaces: each a key space of its own, made on the meta shard's
//! voters with a first shard of its own, and kept apart from the default
//! namespace through crashes; and namespaces a charter names, which take
//! the ids the charter fixes only where the meta shard never handed them
//! out.

use oleaph_meta::{Assurance, Created, Namespace, NamespaceName};
use oleaph_raft::openraft::AsyncRuntime as _;
use oleaph_shard::{NamespaceId, Shard, ShardId};
use oleaph_sim::{Cluster, Faults, Outcome};
use oleaph_storage::WriteBatch;
use openraft_rt_sim::SimRuntime;

#[test]
fn a_namespace_holds_its_own_keys_on_every_voter() {
    SimRuntime::with_seed(31).block_on(async {
        let mut cluster = Cluster::start(31, 3, Faults::default())
            .await
            .expect("start");
        cluster.settle().await.expect("settle");
        let payments: NamespaceName = "payments".parse().expect("a name");
        let first = cluster.create_namespace(&payments).await.expect("created");
        let id = first.namespace();
        assert_ne!(id, NamespaceId::DEFAULT, "a namespace of its own");
        assert_eq!(first.id(), ShardId::new(2), "the next shard id");
        assert_eq!(
            cluster.create_namespace(&payments).await,
            None,
            "the name is taken"
        );
        let default = NamespaceName::default_name();
        assert_eq!(
            cluster.create_namespace(&default).await,
            None,
            "so is the default's"
        );
        cluster.settle().await.expect("settle");
        let put = |value: &str| WriteBatch::new().put("k", value.to_owned());
        assert_eq!(
            cluster.write_in(id, put("paid")).await,
            Outcome::Acked,
            "in"
        );
        assert_eq!(cluster.write(put("plain")).await, Outcome::Acked, "default");
        let registry = cluster.registry().await.expect("a meta leader");
        let recorded = registry.namespace(&payments).expect("recorded");
        assert_eq!(recorded.id(), id, "by name");
        assert_eq!(recorded.assurance(), Assurance::Cft, "crash fault tolerant");
        let leader = cluster.leader().expect("a leader");
        cluster.crash(leader).await.expect("crash");
        assert_eq!(
            cluster.write_in(id, put("again")).await,
            Outcome::Acked,
            "survives"
        );
        cluster.settle().await.expect("settle");
        for (node, holdings) in cluster.states() {
            let state = holdings.shard(first.id()).expect("every voter holds it");
            assert_eq!(state.shard().namespace(), id, "{node}: its namespace");
            assert_eq!(
                holdings.get_in(id, b"k"),
                Some(b"again".as_slice()),
                "{node}: in"
            );
            assert_eq!(
                holdings.get(b"k"),
                Some(b"plain".as_slice()),
                "{node}: apart"
            );
        }
        cluster.shutdown().await.expect("shutdown");
    });
}

#[test]
fn a_chartered_namespace_takes_only_ids_never_handed_out() {
    SimRuntime::with_seed(32).block_on(async {
        let mut cluster = Cluster::start(32, 3, Faults::default())
            .await
            .expect("start");
        cluster.settle().await.expect("settle");
        let name = |name: &str| -> NamespaceName { name.parse().expect("a name") };
        let ns = NamespaceId::new;
        let charter = |called: NamespaceName, namespace: u32, shard: u64| {
            let cluster = &cluster;
            async move {
                cluster
                    .charter_namespace(&called, ns(namespace), ShardId::new(shard))
                    .await
                    .expect("a meta leader")
            }
        };
        let ledger = Shard::whole(ns(7), ShardId::new(40));
        assert_eq!(
            charter(name("ledger"), 7, 40).await,
            Created::Founded(ledger.clone()),
            "recorded"
        );
        assert_eq!(
            charter(name("ledger"), 7, 40).await,
            Created::Founded(ledger.clone()),
            "again, as the first time"
        );
        let chartered = Namespace::new(ns(7), Assurance::Bft);
        assert_eq!(
            charter(name("ledger"), 9, 50).await,
            Created::Taken(chartered),
            "the name is taken"
        );
        assert_eq!(
            charter(NamespaceName::default_name(), 9, 50).await,
            Created::Taken(Namespace::founding()),
            "so is the default's"
        );
        assert_eq!(
            charter(name("audit"), 7, 41).await,
            Created::Spent,
            "the namespace id is taken"
        );
        assert_eq!(
            charter(name("audit"), 9, 40).await,
            Created::Spent,
            "the shard id is taken"
        );
        assert_eq!(
            charter(name("audit"), 8, 41).await,
            Created::Founded(Shard::whole(ns(8), ShardId::new(41))),
            "the next ids are free"
        );
        let registry = cluster.registry().await.expect("a meta leader");
        assert_eq!(
            registry.namespace(&name("ledger")),
            Some(&chartered),
            "by name"
        );
        let map = registry.map(ns(7)).expect("routed");
        assert_eq!(map.locate(b"any key"), &ledger, "all of it");
        let made = cluster
            .create_namespace(&name("payments"))
            .await
            .expect("created");
        assert_eq!(made.namespace(), ns(9), "after the chartered namespaces");
        assert_eq!(made.id(), ShardId::new(42), "after the chartered shards");
        cluster.shutdown().await.expect("shutdown");
    });
}
