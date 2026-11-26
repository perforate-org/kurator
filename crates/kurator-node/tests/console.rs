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
    let mut ask = |line: &str| {
        writeln!(stdin, "{line}").unwrap();
        let mut answer = String::new();
        let _: usize = stdout.read_line(&mut answer).unwrap();
        answer.trim_end().to_owned()
    };
    assert!(ask("help").starts_with("commands:"), "help");
    let started = Instant::now();
    loop {
        let answer = ask("put greeting hello");
        if answer.starts_with("ok at") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "never became the leader: {answer}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(ask("get greeting"), "hello", "read back");
    assert_eq!(ask("get missing"), "(none)", "absent");
    assert!(ask("members").starts_with("voter 1 127.0.0.1:"), "members");
    assert!(
        ask("promote 7").starts_with("error: n7 cannot vote yet"),
        "a stranger cannot be promoted"
    );
    writeln!(stdin, "quit").unwrap();
    assert!(node.wait().unwrap().success(), "quits cleanly");
    let mut rest = String::new();
    let _: usize = stdout.read_to_string(&mut rest).unwrap();
    assert_eq!(rest, "", "says nothing after quit");
}
