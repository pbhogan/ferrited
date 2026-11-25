mod common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::process::{free_port, spawn};
use common::{exchange, start, try_exchange};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Memcached(Child);

impl Drop for Memcached {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts real memcached on a free port, or returns `None` if it is not installed.
async fn start_memcached() -> Option<(Memcached, SocketAddr)> {
    let port = free_port();
    let child = spawn(
        Command::new("memcached")
            .args(["-l", "127.0.0.1", "-p", &port.to_string(), "-U", "0"])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )
    .ok()?;
    let guard = Memcached(child);
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while tokio::net::TcpStream::connect(addr).await.is_err() {
        assert!(Instant::now() < deadline, "memcached did not start");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Some((guard, addr))
}

fn scripts() -> Vec<String> {
    let mut scripts: Vec<String> = [
        // Classic storage and retrieval
        "set a 5 0 3\r\nabc\r\nget a\r\nget a missing\r\n",
        "add b 0 0 1\r\nx\r\nadd b 0 0 1\r\ny\r\nreplace b 0 0 1\r\nz\r\nreplace nope 0 0 1\r\nz\r\nget b\r\n",
        "set c 9 0 3\r\nmid\r\nappend c 0 0 2\r\n>>\r\nprepend c 0 0 2\r\n<<\r\nget c\r\nappend nope 0 0 1\r\nx\r\n",
        "set d 0 -1 1\r\nx\r\nget d\r\n",
        "set e 0 0 1 noreply\r\nx\r\nget e\r\ndelete e noreply\r\nget e\r\n",
        "cas f 0 0 1 1\r\nx\r\n",
        // Delete, arithmetic, touch
        "delete g\r\nset g 0 0 1\r\nx\r\ndelete g\r\ndelete g\r\n",
        "delete g 0\r\ndelete g 5\r\n",
        "incr h 1\r\nset h 0 0 2\r\n10\r\nincr h 5\r\nincr h 100\r\ndecr h 5\r\nget h\r\n",
        "set i 0 0 1\r\n5\r\ndecr i 9\r\n",
        "set j 0 0 20\r\n18446744073709551615\r\nincr j 2\r\n",
        "set k 0 0 3\r\nabc\r\nincr k 1\r\ndecr k 1\r\n",
        "set l 0 0 1\r\n1\r\nincr l abc\r\nincr l -1\r\nincr l\r\n",
        "touch m 10\r\nset m 0 0 1\r\nx\r\ntouch m 10\r\ntouch m abc\r\ngat 0 m\r\ngat -1 m\r\nget m\r\n",
        // Flush
        "set n 0 0 1\r\nx\r\nflush_all\r\nget n\r\nset n 0 0 1\r\ny\r\nget n\r\nflush_all noreply\r\nget n\r\n",
        // Classic errors
        "bogus\r\n",
        "\r\n",
        "get\r\n",
        "set o 0 0\r\n",
        "set o x 0 1\r\nz\r\n",
        "set o 0 0 -1\r\n",
        "set o 0 0 3\r\nabcde\r\n",
        "verbosity\r\nverbosity 1\r\n",
        "shutdown\r\n",
        "shutdown bogus\r\nshutdown graceful\r\nshutdown noreply\r\n shutdown bogus\r\n",
        // Meta basics
        "ms p 3\r\nabc\r\nmg p v\r\nmg p\r\nmg p s f t v\r\nmg missing v\r\n",
        "ms q 3 F7 T0\r\nabc\r\nmg q f v\r\nmg q k v\r\n",
        "mg missing v k O123\r\nmg missing k O123 q\r\n",
        "ms r 1 k O1\r\na\r\nmd r k O2\r\nmd r k O3\r\nma r k O4\r\n",
        "ms s 1 q\r\na\r\nmg s v q\r\nmd s q\r\nmd s q\r\n",
        "ms t 1 MR\r\na\r\nms t 1 ME\r\nb\r\nms t 1 ME\r\nc\r\nms t 1 MA\r\nd\r\nms t 1 MP\r\ne\r\nmg t v\r\n",
        "ms u 1 MA N0\r\nz\r\nmg u v\r\n",
        "ms v 1 C5\r\na\r\nms v 1 E42\r\na\r\nmg v c\r\nms v 1 C41\r\nb\r\nms v 1 C42\r\nb\r\nmd v C1\r\n",
        // Meta arithmetic
        "ma w\r\nma w N0 J10 v\r\nma w v\r\nma w D5 v\r\nma w MD D100 v\r\nma w\r\nma w q\r\nma w v\r\n",
        "ms x 1\r\na\r\nma x\r\n",
        "ma y q\r\nma y N0 q\r\nma y v t\r\n",
        // Meta stampede flags
        "mg z v N30\r\nmg z v N30\r\nms z 1 T100\r\na\r\nmg z v N30\r\nmg z R200\r\nmg z R200\r\n",
        "ms aa 1\r\na\r\nmd aa I T30\r\nmg aa v\r\nmg aa v\r\nms aa 1\r\nb\r\nmg aa v\r\n",
        "ms ab 3\r\nabc\r\nmd ab x\r\nmg ab v\r\n",
        "ms ac 1\r\na\r\nmg ac h u\r\nmg ac h\r\nmg ac h\r\nmg ac T-1\r\nmg ac\r\n",
        // Meta base64 keys and errors
        "ms Zm9vIGJhcg== 1 b\r\na\r\nmg Zm9vIGJhcg== b k v\r\nmg Zm9v b v\r\nmg !!! b v\r\n",
        "mg\r\nmg ad !\r\nmg ad v v\r\nmg ad Tabc\r\nmg ad F5\r\n",
        "ms ae 3 MX\r\nabc\r\nmg ae v\r\n",
        "ms ae abc\r\n",
        "ma af MX\r\n",
        "me missing\r\n",
        // Lines too short to hold a command, and lines that start with a space
        "x\r\n",
        " \r\n",
        "mn\n",
        "xy\r\n",
        "  \r\n",
        " get a\r\n",
        " mn\r\n",
        " version\r\n",
        " verbosity 1\r\n flush_all\r\n",
        // Missing arguments
        "delete\r\nincr\r\ndecr h\r\ntouch\r\ntouch m\r\ngets\r\ngat\r\ngat 1\r\ngats\r\n",
        "set\r\nset o\r\nset o 0\r\nadd o 0 0\r\n",
        // Extra arguments
        "set ba 0 0 1\r\nx\r\ndelete ba abc def\r\nget ba\r\ndelete ba 0 0\r\n",
        "incr bb 1 2 3\r\ntouch bb 1 2 3\r\nflush_all 0 0 0\r\nverbosity 1 2 3\r\n",
        // noreply is the last token, wherever that is, and silences errors too
        "set bc 0 0 1 noreply extra\r\nx\r\nget bc\r\nset bc 0 0 1 x noreply\r\ny\r\nget bc\r\n",
        "set bd 0 0 1\r\nx\r\ndelete bd noreply abc\r\nget bd\r\n",
        "bogus noreply\r\nnoreply\r\nincr be abc noreply\r\ntouch be noreply\r\ntouch be abc noreply\r\n",
        "verbosity noreply\r\nverbosity abc noreply\r\nflush_all abc noreply\r\ndelete noreply\r\n",
        "incr noreply\r\nset bf 0 0 noreply\r\nset bf 0 noreply\r\ngat noreply\r\nget noreply\r\n",
        // A rejected storage line swallows its data block once the length is known
        "set bg -1 0 1\r\nx\r\nset bg 0 abc 1\r\nx\r\nset bg 0 99999999999999999999 1\r\nx\r\nget bg\r\n",
        "cas bh 0 0 1\r\nx\r\ncas bh 0 0 1 abc\r\nx\r\ncas bh 0 0 1 -5\r\nx\r\nget bh\r\n",
        "set bi x 0 1 noreply\r\nz\r\ncas bi 0 0 1 noreply\r\nz\r\nget bi\r\n",
        "set bj 0 0 abc\r\nset bj 0 0 -5\r\nset bj 0 0 +1\r\nset bj 0 0 1x\r\n",
        "set bj 0 0 2147483648\r\nset bj 0 0 99999999999999999999\r\n",
        // Bad data chunks
        "set bk 0 0 1\r\nx\r\nset bk 0 0 3\r\nabcdefgh\r\nget bk\r\n",
        "set bl 0 0 1\r\nx\r\nset bl 0 0 3 noreply\r\nabcde\r\nget bl\r\n",
        "ms bm 1\r\nx\r\nms bm 3 q\r\nabcde\r\nmg bm v\r\n",
        // Numbers
        "touch bn +5\r\ntouch bn 4294967296\r\ntouch bn 1x\r\nincr bn +5\r\nincr bn 1x\r\ngat 1x bn\r\n",
        "flush_all 1x\r\nflush_all noreply 0\r\nverbosity -1\r\nverbosity 1x\r\nverbosity abc\r\n",
        "set bo +5 +0 1\r\nx\r\nget bo\r\n",
        "stats bogus\r\n",
        "stats reset\r\nstats reset extra\r\nstats  reset\r\n",
        // Meta: flag tokens the tokenizer rejects, unknown flags, and flags other commands own
        "mg bp @\r\nmg bp {\r\nmd bp !\r\nma bp !\r\nme bp !\r\nms bp 1 !\r\n",
        "mg bq Z\r\nmg bq 5\r\nmg bq [\r\nmg bq _\r\nmd bq Z\r\nms bq 1 S1\r\nz\r\nmg bq v\r\n",
        "mg br D5 J5 x I C5 F5 MX\r\nmd br D5 J5 N5 F5 M5 R5 s t v\r\nma br F5 s\r\nme br Z 5\r\n",
        "ms bs 1 D5 J5 R5 v x u h l t f\r\nz\r\nmg bs v\r\nma bt N0 v s f h l\r\n",
        // Meta: which error is reported when several apply
        "mg bu Tabc Z\r\nmg bu Tabc v v\r\nmg !!! b Tabc\r\nmg !!! Tabc b\r\nmg bu Tabc Fabc\r\n",
        "mg bu Dabc\r\nmg bu Jabc\r\nmg bu Fabc\r\nmg bu T+5\r\nmg bu T\r\nmg bu E-1\r\nmg bu Cabc\r\n",
        "ms bv 1 Tabc Fabc\r\nz\r\nms bv 1 Fabc\r\nz\r\nms bv 1 F-1\r\nz\r\nms bv 1 F4294967296\r\nz\r\nmg bv v\r\n",
        "ma\r\nma !!! b\r\nma bw v v\r\nma bw Z\r\nma bw Dabc\r\nma bw D\r\nma bw Jabc N0\r\nma bw Tabc\r\nma bw Cabc\r\n",
        // Meta: modes
        "ma bx M\r\nma bx Mi\r\nma bx Md\r\nma bx Mincr\r\nma bx N0 MI v\r\nma bx M+ v\r\nma bx MD v\r\nma bx M- v\r\n",
        "ms by 1 MS\r\na\r\nms by 1 Ms\r\nb\r\nms by 1 Me\r\nb\r\nms by 1 M\r\nb\r\nms by 1 Mset\r\nb\r\nmg by v\r\n",
        // Meta: ms without a usable length swallows nothing
        "ms\r\nms bz\r\nms bz -1\r\nms bz 1x\r\nms bz S1\r\nms bz 2147483648\r\n",
        // Meta: ms return flags on every status, and the size after append and prepend
        "ms ca 1\r\na\r\nms ca 2 ME s k O1 q\r\nbb\r\nms ca 2 ME c\r\nbb\r\nms ca 1 C1 s k\r\nz\r\n",
        "ms cb 1 MR s k c\r\nz\r\nms cb 1 MA s k\r\nz\r\n",
        "ms cc 3\r\nabc\r\nms cc 2 MA s\r\nde\r\nms cc 2 MP s\r\nzz\r\nmg cc s v\r\n",
        // Meta: base64 keys that do not decode
        "ms !!! 1 b\r\nz\r\nmg b b\r\nmg YQ b k\r\nmg YWJ b\r\nmg ==== b\r\nmd !!! b\r\nmg YQ== b k\r\n",
        // Meta: a matching C on mg skips the value
        "ms cd 1 E42\r\na\r\nmg cd v C42\r\nmg cd v C41\r\nmg cd C42 s\r\n",
        // Meta: what q suppresses
        "mg ce v q\r\nms ce 1\r\na\r\nmg ce v q\r\nmg ce q\r\nmg ce k q s\r\nmd ce I q\r\nmd ce q\r\nmd ce q k\r\n",
        // Meta: md x leaves a new empty item
        "ms cf 3 F5 T100\r\nabc\r\nmd cf x\r\nmg cf s f v t\r\nmd cf x q\r\nmd cf q k\r\nmd cf q k\r\n",
        "ms cg 3 F5\r\nabc\r\nmd cg x F7\r\nmg cg s f v t\r\n",
        "ms ch 3 F5 T100\r\nabc\r\nmd ch x I\r\nmg ch s f v t\r\nmg ch s v\r\n",
        // Meta: order of the W, X and Z flags
        "ms ci 1 T100\r\na\r\nmg ci R200 v\r\nmg ci R200 v\r\nmg ci v\r\nmd ci I\r\nmg ci s v O1\r\nmg ci v\r\n",
    ]
    .iter()
    .map(|script| script.to_string())
    .collect();

    // Oversized values: rejected, swallowed, and the old value removed.
    let big = "x".repeat(2 * 1024 * 1024);
    scripts.push(format!(
        "set big 0 0 1\r\na\r\nset big 0 0 {}\r\n{big}\r\nget big\r\n",
        big.len()
    ));
    scripts.push(format!(
        "set big 0 0 1\r\na\r\nadd big 0 0 {}\r\n{big}\r\nget big\r\n",
        big.len()
    ));
    scripts.push(format!(
        "ms big 1\r\na\r\nms big {}\r\n{big}\r\nmg big v\r\n",
        big.len()
    ));
    // Only a plain set removes the old value, and only noreply silences the error.
    scripts.push(format!(
        "set big 0 0 1\r\na\r\ncas big 0 0 {} 1\r\n{big}\r\nget big\r\n",
        big.len()
    ));
    scripts.push(format!(
        "set big 0 0 {} noreply\r\n{big}\r\nget big\r\n",
        big.len()
    ));
    scripts.push(format!(
        "ms big 1\r\na\r\nms big {} q\r\n{big}\r\nmg big v\r\n",
        big.len()
    ));
    // Key length limit. Oversized-key `get`, `gets` and `gat` are left out: memcached adds an
    // empty line after the error.
    let long_key = "k".repeat(251);
    scripts.push(format!("set {long_key} 0 0 1\r\nx\r\nmg {long_key} v\r\n"));
    scripts.push(format!(
        "delete {long_key}\r\nincr {long_key} 1\r\ntouch {long_key} 1\r\n"
    ));
    scripts.push(format!(
        "md {long_key}\r\nma {long_key}\r\nme {long_key}\r\nms {long_key} 1\r\nx\r\n"
    ));
    let key = "k".repeat(250);
    scripts.push(format!("set {key} 0 0 1\r\nx\r\nmg {key} v\r\n"));
    // A meta command line holds at most 20 tokens.
    let flags = "s t f k h l q u E1 N0 R1 T1 O1 P L v x";
    scripts.push(format!("mg fl {flags} I\r\nmg fl {flags} I D1\r\nms fl 1 {flags}\r\na\r\nms fl 1 {flags} I\r\na\r\n"));
    // Opaque tokens are limited to 32 bytes, counting the O.
    let opaque = "a".repeat(31);
    scripts.push(format!("mg op O{opaque}\r\nmg op O{opaque}a\r\nmd op O{opaque}a\r\nms op 1 O{opaque}a\r\na\r\nmg op v\r\n"));
    // Meta: a CAS token with every set mode, on a missing key and on an existing one.
    let modes = ["", " MS", " ME", " MR", " MA", " MP"];
    for (index, mode) in modes.iter().enumerate() {
        for (variant, extra) in ["", " N30", " I"].iter().enumerate() {
            let key = format!("cm{index}{variant}");
            scripts.push(format!("ms {key} 1{mode} C5{extra}\r\nx\r\nmg {key} v\r\n"));
        }
        for (variant, token) in ["C100", "C5", "C5 I", "C500 I"].iter().enumerate() {
            let key = format!("ce{index}{variant}");
            scripts.push(format!(
                "ms {key} 1 E100\r\na\r\nms {key} 1{mode} {token} s\r\nx\r\nmg {key} v\r\n"
            ));
        }
    }
    // Arithmetic on values that are not plain digits. Not read back: memcached space-pads a
    // shortened number in place.
    for value in [
        " 5",
        "5 ",
        " 5 ",
        "\t5",
        "5\t",
        "5\n",
        "\n5",
        "5\r",
        "\u{b}5\u{c}",
        "+5",
        " +5",
        "+ 5",
        "-5",
        " -5",
        "-0",
        "5a",
        "5 a",
        "5 6",
        "5-",
        "05",
        "0x5",
        "5.0",
        "1e3",
        "5\0",
        "5\0abc",
        "\x005",
        "",
        "00000000000000000000005",
        "5                      ",
        " 18446744073709551615",
        "18446744073709551616",
        "-18446744073709551615",
        "-18446744073709551616",
        "-9223372036854775808",
        "-9223372036854775809",
        "18446744073709551615 -",
    ] {
        scripts.push(format!(
            "set num 0 0 {}\r\n{value}\r\nincr num 1\r\ndecr num 1\r\nma num v\r\nma num MD v\r\n",
            value.len()
        ));
    }
    // A multi-key get far longer than an ordinary command line.
    let key = "k".repeat(100);
    scripts.push(format!(
        "set {key} 0 0 1\r\nx\r\nget{}\r\n",
        format!(" {key}").repeat(3_000)
    ));
    scripts
}

#[tokio::test]
async fn replies_match_memcached() {
    let Some((_guard, memcached)) = start_memcached().await else {
        // Passing without comparing anything would hide a missing dependency.
        assert!(
            std::env::var("FERRITED_SKIP_PARITY").as_deref() == Ok("1"),
            "memcached is not installed, so replies cannot be compared against it. \
             Install memcached, or set FERRITED_SKIP_PARITY=1 to skip the parity tests."
        );
        eprintln!("memcached is not installed and FERRITED_SKIP_PARITY=1; skipping parity tests");
        return;
    };
    let ferrited = start().await;
    let version = exchange(memcached, "version\r\n").await;
    let version = version.trim_end();
    let mut failures = Vec::new();
    for script in scripts() {
        // An older memcached may not answer a script in full; report that as a difference.
        let expected = try_exchange(memcached, &script)
            .await
            .unwrap_or_else(|problem| format!("<{problem}>"));
        let actual = exchange(ferrited, &script).await;
        if expected != actual {
            let shown: String = script.chars().take(200).collect();
            failures.push(format!(
                "script:    {shown:?}\nmemcached: {expected:?}\nferrited:  {actual:?}\n"
            ));
        }
    }
    // Checked apart from the scripts, where its `MN` reply would race the sentinel.
    for server in [memcached, ferrited] {
        let mut stream = tokio::net::TcpStream::connect(server).await.unwrap();
        stream.write_all(b"mn x y\r\nmn\r\n").await.unwrap();
        let mut reply = [0u8; 8];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut reply))
            .await
            .expect("timed out waiting for two MN replies")
            .unwrap();
        if &reply != b"MN\r\nMN\r\n" {
            failures.push(format!(
                "mn x y on {server}: {:?}",
                String::from_utf8_lossy(&reply)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} scripts differ from {version} (they were checked against memcached 1.6.45):\n\n{}",
        failures.len(),
        failures.join("\n")
    );
}
