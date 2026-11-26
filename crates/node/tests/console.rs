//! The `kurator` binary, driven through its console like an operator would.
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const KURATOR: &str = env!("CARGO_BIN_EXE_kurator");

#[test]
fn key_prints_the_same_key_every_time() {
    let dir = tempfile::tempdir().unwrap();
    let key = || {
        let output = Command::new(KURATOR)
            .arg("key")
            .arg(dir.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "key succeeds");
        String::from_utf8(output.stdout).unwrap()
    };
    let first = key();
    assert_eq!(first.trim().len(), 64, "64 hex digits: {first:?}");
    assert_eq!(key(), first, "stable");
}

#[test]
fn a_bad_command_line_fails_with_a_message() {
    let output = Command::new(KURATOR).arg("serve").output().unwrap();
    assert!(!output.status.success(), "fails");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.starts_with("kurator: expected"),
        "explains: {stderr}"
    );
    assert!(output.stdout.is_empty(), "nothing on stdout");
}

#[test]
fn a_single_node_cluster_takes_writes_and_reads_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = Command::new(KURATOR)
        .args([
            "run",
            "--id",
            "1",
            "--listen",
            "127.0.0.1:0",
            "--found",
            "--dir",
        ])
        .arg(dir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = node.stdin.take().unwrap();
    let mut stdout = BufReader::new(node.stdout.take().unwrap());
    let mut banner = String::new();
    let _: usize = stdout.read_line(&mut banner).unwrap();
    assert!(banner.starts_with("ready"), "banner: {banner}");
    // Each answer ends in a line break; one that spans `lines` lines comes
    // back whole.
    let mut ask = |line: &str, lines: usize| {
        writeln!(stdin, "{line}").unwrap();
        let mut answer = String::new();
        for _ in 0..lines {
            let _: usize = stdout.read_line(&mut answer).unwrap();
        }
        answer.trim_end().to_owned()
    };
    assert!(ask("help", 1).starts_with("commands:"), "help");
    let started = Instant::now();
    loop {
        let answer = ask("put greeting hello", 1);
        if answer.starts_with("ok at") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "never became the leader: {answer}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(ask("get greeting", 1), "hello", "read back");
    assert_eq!(ask("get missing", 1), "(none)", "absent");
    assert_eq!(ask("shards", 1), "s1 [, ..) at e0", "one shard");
    let members = ask("members", 2);
    assert!(
        members.starts_with("s1 [, ..) at e0\nvoter 1 127.0.0.1:"),
        "members: {members}"
    );
    assert!(
        ask("promote 7", 1).starts_with("error on s1: n7 cannot vote yet"),
        "a stranger cannot be promoted"
    );
    assert_eq!(
        ask("split m 2", 1),
        "ok: s1 [, m) at e1 | s2 [m, ..) at e1",
        "split"
    );
    let split = Instant::now();
    loop {
        let answer = ask("put melon green", 1);
        if answer.starts_with("ok at") {
            break;
        }
        assert!(
            split.elapsed() < Duration::from_secs(20),
            "the new shard never elected a leader: {answer}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(ask("get melon", 1), "green", "read from the new shard");
    assert_eq!(ask("get greeting", 1), "hello", "still in the first");
    writeln!(stdin, "quit").unwrap();
    assert!(node.wait().unwrap().success(), "quits cleanly");
    let mut rest = String::new();
    let _: usize = stdout.read_to_string(&mut rest).unwrap();
    assert_eq!(rest, "", "says nothing after quit");
}

#[test]
fn a_node_that_holds_no_shard_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = Command::new(KURATOR)
        .args(["run", "--id", "2", "--listen", "127.0.0.1:0", "--dir"])
        .arg(dir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = node.stdin.take().unwrap();
    let mut stdout = BufReader::new(node.stdout.take().unwrap());
    let mut banner = String::new();
    let _: usize = stdout.read_line(&mut banner).unwrap();
    assert!(banner.starts_with("ready"), "banner: {banner}");
    for line in ["get k", "put k v", "del k", "split k 2"] {
        writeln!(stdin, "{line}").unwrap();
        let mut answer = String::new();
        let _: usize = stdout.read_line(&mut answer).unwrap();
        assert_eq!(
            answer.trim_end(),
            "error: no shard on this node holds the key",
            "{line}"
        );
    }
    writeln!(stdin, "quit").unwrap();
    assert!(node.wait().unwrap().success(), "quits cleanly");
}
