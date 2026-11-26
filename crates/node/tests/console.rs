//! The `kurator` binary, driven through its console like an operator would.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test code: a failure should panic with context"
)]
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
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

/// How long the console may take to answer one line: a node that hangs
/// fails the test instead of hanging it.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// A running node, driven through its console.
struct Console {
    node: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
}

impl Console {
    /// Runs `kurator run` with `args` and `--dir <dir>`, and waits for it
    /// to say it is ready.
    fn start(args: &[&str], dir: &Path) -> Self {
        let mut node = Command::new(KURATOR)
            .arg("run")
            .args(args)
            .arg("--dir")
            .arg(dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = node.stdin.take().unwrap();
        let stdout = BufReader::new(node.stdout.take().unwrap());
        let (send, lines) = mpsc::channel();
        // Stops at the end of the output, or once the console is gone.
        drop(std::thread::spawn(move || {
            let _sent: Result<(), _> = stdout
                .lines()
                .map_while(Result::ok)
                .try_for_each(|line| send.send(line));
        }));
        let console = Self { node, stdin, lines };
        let banner = console.read(1);
        assert!(banner.starts_with("ready"), "banner: {banner}");
        console
    }

    /// Sends `line` and reads an answer of `lines` lines.
    fn ask(&mut self, line: &str, lines: usize) -> String {
        writeln!(self.stdin, "{line}").unwrap();
        self.read(lines)
    }

    fn read(&self, lines: usize) -> String {
        let answer: Vec<String> = (0..lines)
            .map(|_| {
                self.lines
                    .recv_timeout(ANSWER_TIMEOUT)
                    .unwrap_or_else(|error| panic!("no answer: {error}"))
            })
            .collect();
        answer.join("\n")
    }

    /// Says `quit`, checks that the node stops cleanly, and returns what it
    /// said after.
    fn quit(mut self) -> Vec<String> {
        writeln!(self.stdin, "quit").unwrap();
        assert!(self.node.wait().unwrap().success(), "quits cleanly");
        self.lines.iter().collect()
    }
}

#[test]
fn a_single_node_cluster_takes_writes_and_reads_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut console = Console::start(
        &["--id", "1", "--listen", "127.0.0.1:0", "--found"],
        dir.path(),
    );
    let mut ask = |line: &str, lines: usize| console.ask(line, lines);
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
    // The meta shard elects its own leader.
    let mut shards = ask("shards", 1);
    while shards.starts_with("error") && started.elapsed() < Duration::from_secs(20) {
        std::thread::sleep(Duration::from_millis(50));
        shards = ask("shards", 1);
    }
    assert_eq!(shards, "s1 [, ..) at e0 on n1", "one shard");
    let members = ask("members", 4);
    assert!(
        members.starts_with("s0 [, ..) at e0\nvoter 1 127.0.0.1:"),
        "the meta shard: {members}"
    );
    assert!(
        members.contains("\ns1 [, ..) at e0\nvoter 1 127.0.0.1:"),
        "the first shard: {members}"
    );
    assert!(
        ask("promote 7", 1).starts_with("error on s0: n7 cannot vote yet"),
        "a stranger cannot be promoted"
    );
    assert_eq!(
        ask("split m", 1),
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
    assert_eq!(
        ask("shards", 2),
        "s1 [, m) at e1 on n1\ns2 [m, ..) at e1 on n1",
        "the registry records both halves"
    );
    assert!(console.quit().is_empty(), "says nothing after quit");
}

#[test]
fn a_node_that_holds_no_shard_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let mut console = Console::start(&["--id", "2", "--listen", "127.0.0.1:0"], dir.path());
    for line in ["get k", "put k v", "del k", "split k"] {
        assert_eq!(
            console.ask(line, 1),
            "error: no shard on this node holds the key",
            "{line}"
        );
    }
    assert!(console.quit().is_empty(), "nothing more");
}
