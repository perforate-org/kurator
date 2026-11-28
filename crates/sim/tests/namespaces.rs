//! Namespaces: each a key space of its own, made on the meta shard's
//! voters with a first shard of its own, and kept apart from the default
//! namespace through crashes.

use oleaph_meta::{Assurance, NamespaceName};
use oleaph_raft::openraft::AsyncRuntime as _;
use oleaph_shard::{NamespaceId, ShardId};
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
