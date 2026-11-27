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
    assert!(
        first.starts_with("ed25519:"),
        "names its algorithm: {first:?}"
    );
    assert_eq!(first.trim().len(), 8 + 64, "and 64 hex digits: {first:?}");
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
    assert_eq!(
        console.ask("shards", 1),
        "error: this node holds no replica of the meta shard",
        "no registry to read"
    );
    assert!(console.quit().is_empty(), "nothing more");
}

/// The key `kurator key` prints for `dir`.
fn key_of(dir: &Path) -> String {
    let output = Command::new(KURATOR).arg("key").arg(dir).output().unwrap();
    assert!(output.status.success(), "key succeeds");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

/// Runs `kurator client` with `args`: whether it succeeded, and what it
/// printed on stdout and stderr.
fn client(args: &[&str]) -> (bool, String) {
    let output = Command::new(KURATOR)
        .arg("client")
        .args(args)
        .output()
        .unwrap();
    let mut shown = String::from_utf8(output.stdout).unwrap();
    shown.push_str(&String::from_utf8(output.stderr).unwrap());
    (output.status.success(), shown.trim().to_owned())
}

#[test]
fn a_trusted_client_runs_a_transaction_across_shards() {
    let (node_dir, client_dir, stranger_dir) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let node_key = key_of(node_dir.path());
    let client_key = key_of(client_dir.path());
    let mut console = Console::start(
        &[
            "--id",
            "1",
            "--listen",
            "127.0.0.1:0",
            "--client",
            &client_key,
            "--found",
        ],
        node_dir.path(),
    );
    let started = Instant::now();
    while !console.ask("put a 0", 1).starts_with("ok at") {
        assert!(started.elapsed() < Duration::from_secs(20), "no leader");
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut split = console.ask("split m", 1);
    while !split.starts_with("ok") {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "no split: {split}"
        );
        std::thread::sleep(Duration::from_millis(50));
        split = console.ask("split m", 1);
    }
    // The meta shard and both halves, each with its one voter.
    let members = console.ask("members", 6);
    let addr = members
        .lines()
        .nth(1)
        .and_then(|voter| voter.split_whitespace().nth(2))
        .unwrap_or_else(|| panic!("an address: {members}"))
        .to_owned();
    let peer = format!("1={addr},{node_key}");
    let dir = client_dir.path().to_str().unwrap();
    let mut wrote = client(&[
        "--dir", dir, "--peer", &peer, "put", "a", "1", "put", "z", "2",
    ]);
    while !wrote.1.starts_with("committed at") {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "no commit: {wrote:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
        wrote = client(&[
            "--dir", dir, "--peer", &peer, "put", "a", "1", "put", "z", "2",
        ]);
    }
    assert!(wrote.0, "succeeds");
    let (read, shown) = client(&[
        "--dir", dir, "--peer", &peer, "get", "a", "get", "z", "get", "q",
    ]);
    assert!(read, "reads: {shown}");
    let lines: Vec<&str> = shown.lines().collect();
    assert_eq!(
        lines.get(..3),
        Some(["a = 1", "z = 2", "q = (none)"].as_slice()),
        "{shown}"
    );
    assert_eq!(console.ask("get z", 1), "2", "the node sees it");
    assert_eq!(console.ask("collect", 1), "ok: forgotten", "collected");
    let stranger = stranger_dir.path().to_str().unwrap();
    let (refused, said) = client(&["--dir", stranger, "--peer", &peer, "get", "a"]);
    assert!(!refused, "a stranger is not served");
    assert_eq!(said, "kurator: no node answered", "and is told so");
    assert!(console.quit().is_empty(), "nothing more");
}

#[test]
fn a_node_splits_a_shard_that_outgrows_its_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let mut console = Console::start(
        &[
            "--id",
            "1",
            "--listen",
            "127.0.0.1:0",
            "--split-above",
            "512",
            "--found",
        ],
        dir.path(),
    );
    let started = Instant::now();
    let value = "v".repeat(100);
    for key in ["a", "b", "c", "d", "e", "f"] {
        let mut answer = console.ask(&format!("put {key} {value}"), 1);
        while !answer.starts_with("ok at") {
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "no leader: {answer}"
            );
            std::thread::sleep(Duration::from_millis(50));
            answer = console.ask(&format!("put {key} {value}"), 1);
        }
    }
    // Six keys of about 120 bytes: the node splits at the middle, d, on its
    // next check, and the halves are under the ceiling.
    loop {
        let shards = console.ask("shards", 1);
        if shards.starts_with("s1 [, d) at e1") {
            assert_eq!(console.read(1), "s2 [d, ..) at e1 on n1", "{shards}");
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(40),
            "never split: {shards}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(console.quit().is_empty(), "nothing more");
}

/// Asks `line` until the answer starts with `expected`, for up to 20
/// seconds: until a shard elects its leader, say. Returns the answer.
fn ask_until(console: &mut Console, line: &str, lines: usize, expected: &str) -> String {
    let started = Instant::now();
    loop {
        let answer = console.ask(line, lines);
        if answer.starts_with(expected) {
            return answer;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{line}: {answer}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn a_backup_restores_as_a_new_cluster() {
    let (old, new, backups) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let backup = backups.path().join("monday");
    let backup_arg = backup.to_str().unwrap();
    let mut first = Console::start(
        &["--id", "1", "--listen", "127.0.0.1:0", "--found"],
        old.path(),
    );
    drop(ask_until(&mut first, "put apple red", 1, "ok at"));
    drop(ask_until(&mut first, "split m", 1, "ok"));
    drop(ask_until(&mut first, "put melon green", 1, "ok at"));
    assert_eq!(
        first.ask(&format!("backup {backup_arg}"), 1),
        format!("ok: 3 shards in {backup_arg}"),
        "the meta shard and both halves"
    );
    assert!(
        first
            .ask(&format!("backup {backup_arg}"), 1)
            .starts_with("error"),
        "not over a backup"
    );
    assert!(first.quit().is_empty(), "nothing more");

    let mut second = Console::start(
        &[
            "--id",
            "2",
            "--listen",
            "127.0.0.1:0",
            "--restore",
            backup_arg,
        ],
        new.path(),
    );
    assert_eq!(ask_until(&mut second, "get apple", 1, "red"), "red", "left");
    assert_eq!(
        ask_until(&mut second, "get melon", 1, "green"),
        "green",
        "right"
    );
    drop(ask_until(&mut second, "put kiwi brown", 1, "ok at"));
    assert_eq!(
        ask_until(&mut second, "shards", 2, "s1 [, m) at e1 on n2"),
        "s1 [, m) at e1 on n2\ns2 [m, ..) at e1 on n2",
        "the new node holds every shard"
    );
    assert_eq!(
        ask_until(&mut second, "split t", 1, "ok"),
        "ok: s2 [m, t) at e2 | s3 [t, ..) at e2",
        "ids go on from the backup's"
    );
    assert!(second.quit().is_empty(), "nothing more");
    // Restarting with the same flag goes on from what the node holds.
    let mut again = Console::start(
        &[
            "--id",
            "2",
            "--listen",
            "127.0.0.1:0",
            "--restore",
            backup_arg,
        ],
        new.path(),
    );
    assert_eq!(
        ask_until(&mut again, "get kiwi", 1, "brown"),
        "brown",
        "kept"
    );
    assert!(again.quit().is_empty(), "nothing more");
}

/// Asks the node at `addr` for `path` over HTTP; the whole response.
fn http_get(addr: &str, path: &str) -> String {
    use std::io::Read as _;
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    write!(stream, "GET {path} HTTP/1.1\r\nhost: kurator\r\n\r\n").unwrap();
    let mut response = String::new();
    let _read = stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn a_node_serves_its_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let mut console = Console::start(
        &[
            "--id",
            "1",
            "--listen",
            "127.0.0.1:0",
            "--metrics",
            "127.0.0.1:0",
            "--found",
        ],
        dir.path(),
    );
    let announced = console.read(1);
    let addr = announced
        .strip_prefix("metrics at http://")
        .and_then(|rest| rest.strip_suffix("/metrics"))
        .unwrap_or_else(|| panic!("an address: {announced}"))
        .to_owned();
    drop(ask_until(&mut console, "put apple red", 1, "ok at"));
    let page = http_get(&addr, "/metrics");
    assert!(page.starts_with("HTTP/1.1 200 OK\r\n"), "{page}");
    for expected in [
        "content-type: text/plain; version=0.0.4\r\n",
        "# TYPE kurator_shard_leader gauge\n",
        "kurator_shard_leader{shard=\"1\"} 1\n",
        "kurator_shard_keys{shard=\"1\"} 1\n",
        "# TYPE kurator_wal_sync_seconds histogram\n",
    ] {
        assert!(page.contains(expected), "{expected:?} in {page}");
    }
    for family in ["kurator_wal_sync_seconds", "kurator_commit_seconds"] {
        let prefix = format!("{family}_count{{shard=\"1\"}} ");
        let count = page
            .lines()
            .find_map(|line| line.strip_prefix(prefix.as_str()))
            .and_then(|count| count.parse::<u64>().ok());
        assert!(count.is_some_and(|count| count > 0), "{family}: {page}");
    }
    assert!(
        http_get(&addr, "/").starts_with("HTTP/1.1 404 Not Found\r\n"),
        "elsewhere"
    );
    assert!(console.quit().is_empty(), "nothing more");
}
