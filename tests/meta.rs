mod common;

use common::{exchange, send, start};
use tokio::net::TcpStream;

#[tokio::test]
async fn set_and_get_with_return_flags() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 3 F7 T0\r\nabc\r\nmg k s v f t k Oq1\r\nmg k\r\nmg missing v\r\n",
    )
    .await;
    assert_eq!(reply, "HD\r\nVA 3 s3 f7 t-1 kk Oq1\r\nabc\r\nHD\r\nEN\r\n");
}

#[tokio::test]
async fn opaque_and_key_are_echoed_on_every_reply() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "mg missing v k O9\r\nms k 1 k O1\r\na\r\nmd k k O2\r\nmd k k O3\r\nma n k O4\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "EN kmissing O9\r\nHD kk O1\r\nHD kk O2\r\nNF kk O3\r\nNF kn O4\r\n"
    );
}

#[tokio::test]
async fn quiet_mode_suppresses_the_common_reply() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 1 q\r\na\r\nmg missing v q\r\nmg k v q\r\nmd k q\r\nmd k q\r\n",
    )
    .await;
    assert_eq!(reply, "VA 1\r\na\r\nNF\r\n");
}

#[tokio::test]
async fn set_modes() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 1 MR\r\na\r\nms k 1 ME\r\nb\r\nms k 1 ME\r\nc\r\nms k 1 MA\r\nd\r\nms k 1 MP\r\ne\r\nmg k v\r\n\
         ms fresh 1 MA N0\r\nz\r\nmg fresh v\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "NS\r\nHD\r\nNS\r\nHD\r\nHD\r\nVA 3\r\nebd\r\nHD\r\nVA 1\r\nz\r\n"
    );
}

#[tokio::test]
async fn cas_flow() {
    let addr = start().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    assert_eq!(send(&mut stream, "ms k 1 C5\r\na\r\n").await, "NF\r\n");
    let reply = send(&mut stream, "ms k 1 c\r\na\r\n").await;
    let token: u64 = reply
        .trim_end()
        .strip_prefix("HD c")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        send(&mut stream, "mg k c\r\n").await,
        format!("HD c{token}\r\n")
    );
    assert_eq!(
        send(&mut stream, &format!("ms k 1 C{}\r\nb\r\n", token + 1)).await,
        "EX\r\n"
    );
    assert_eq!(
        send(&mut stream, &format!("ms k 1 C{token}\r\nb\r\n")).await,
        "HD\r\n"
    );
    assert_eq!(
        send(&mut stream, &format!("md k C{token}\r\n")).await,
        "EX\r\n"
    );
    assert_eq!(
        send(&mut stream, "ms j 1 E42\r\na\r\nmg j c\r\n").await,
        "HD\r\nHD c42\r\n"
    );
}

#[tokio::test]
async fn arithmetic() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ma n\r\nma n N0 J10 v\r\nma n v\r\nma n D5 v t\r\nma n MD D100 v\r\nma n\r\nma n q\r\nmg n v\r\n\
         ms text 1\r\na\r\nma text\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "NF\r\nVA 2\r\n10\r\nVA 2\r\n11\r\nVA 2 t-1\r\n16\r\nVA 1\r\n0\r\nHD\r\nVA 1\r\n2\r\n\
         HD\r\nCLIENT_ERROR cannot increment or decrement non-numeric value\r\n"
    );
}

#[tokio::test]
async fn vivify_and_recache_hand_out_one_win() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "mg k v N30\r\nmg k v N30\r\nms k 1 T100\r\na\r\nmg k v N30\r\nmg k R200\r\nmg k R200\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "VA 0 W\r\n\r\nVA 0 Z\r\n\r\nHD\r\nVA 1\r\na\r\nHD W\r\nHD Z\r\n"
    );
}

#[tokio::test]
async fn invalidate_marks_stale() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 1\r\na\r\nmd k I T30\r\nmg k v t\r\nmg k v\r\nms k 1\r\nb\r\nmg k v\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "HD\r\nHD\r\nVA 1 t30 X W\r\na\r\nVA 1 Z X\r\na\r\nHD\r\nVA 1\r\nb\r\n"
    );
}

#[tokio::test]
async fn delete_keeping_an_empty_item() {
    let addr = start().await;
    let reply = exchange(addr, "ms k 3\r\nabc\r\nmd k x\r\nmg k v\r\n").await;
    assert_eq!(reply, "HD\r\nHD\r\nVA 0\r\n\r\n");
}

#[tokio::test]
async fn base64_keys() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms Zm9vIGJhcg== 1 b\r\na\r\nmg Zm9vIGJhcg== b k v\r\nmg Zm9v b v\r\n",
    )
    .await;
    assert_eq!(reply, "HD\r\nVA 1 kZm9vIGJhcg== b\r\na\r\nEN\r\n");
}

#[tokio::test]
async fn hit_and_idle_flags_and_touch() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 1\r\na\r\nmg k h l u\r\nmg k h\r\nmg k h T-1\r\nmg k\r\n",
    )
    .await;
    assert_eq!(reply, "HD\r\nHD h0 l0\r\nHD h0\r\nHD h1\r\nEN\r\n");
}

#[tokio::test]
async fn debug_command() {
    let addr = start().await;
    let reply = exchange(addr, "ms k 3 E7\r\nabc\r\nme k\r\nme missing\r\n").await;
    assert_eq!(
        reply,
        "HD\r\nME k exp=-1 la=0 cas=7 fetch=no cls=1 size=3\r\nEN\r\n"
    );
}

#[tokio::test]
async fn errors() {
    let addr = start().await;
    let reply = exchange(addr, "mg k !\r\nmg k v v\r\nms k 3 MX\r\nabc\r\nmg k v\r\n").await;
    assert_eq!(
        reply,
        "CLIENT_ERROR bad command line format\r\nCLIENT_ERROR duplicate flag\r\nCLIENT_ERROR invalid mode for ms M token\r\nEN\r\n"
    );
}

#[tokio::test]
async fn ms_return_flags_on_every_status() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 3\r\nabc\r\nms k 2 MA s\r\nde\r\nms k 2 ME s c k\r\nzz\r\nms nope 1 MR c s\r\nz\r\n",
    )
    .await;
    assert_eq!(reply, "HD\r\nHD s5\r\nNS s2 c0 kk\r\nNS c0 s1\r\n");
}

#[tokio::test]
async fn mg_with_a_matching_cas_skips_the_value() {
    let addr = start().await;
    let reply = exchange(addr, "ms k 1 E42\r\na\r\nmg k v C42\r\nmg k v C41\r\n").await;
    assert_eq!(reply, "HD\r\nHD\r\nVA 1\r\na\r\n");
}

#[tokio::test]
async fn md_x_leaves_a_new_empty_item() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 3 F5 T100\r\nabc\r\nmd k x\r\nmg k s f t v\r\nmd k x I F7 T50\r\nmg k f t\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "HD\r\nHD\r\nVA 0 s0 f0 t-1\r\n\r\nHD\r\nHD f7 t50 X W\r\n"
    );
}

#[tokio::test]
async fn ma_creates_with_the_later_of_n_and_t() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ma a N0 T30 t\r\nma b T40 N20 t\r\nma c N20 t\r\nma d T30 t\r\n",
    )
    .await;
    assert_eq!(reply, "HD t30\r\nHD t20\r\nHD t20\r\nNF\r\n");
}

#[tokio::test]
async fn pipelined_quiet_gets_end_with_noop() {
    let addr = start().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send(&mut stream, "ms a 1\r\n1\r\nms c 1\r\n3\r\n").await;
    let reply = send(&mut stream, "mg a v k q\r\nmg b v k q\r\nmg c v k q\r\n").await;
    assert_eq!(reply, "VA 1 ka\r\n1\r\nVA 1 kc\r\n3\r\n");
}

fn stat_value(stats: &str, name: &str) -> u64 {
    let prefix = format!("STAT {name} ");
    let line = stats.lines().find(|line| line.starts_with(&prefix));
    line.unwrap_or_else(|| panic!("no {name} in {stats}"))[prefix.len()..]
        .parse()
        .unwrap()
}

#[tokio::test]
async fn ms_with_a_cas_token_updates_the_cas_counters() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 1 C5\r\na\r\nms k 1 E100\r\na\r\nms k 1 C5\r\nb\r\nms k 1 C100\r\nb\r\n",
    )
    .await;
    assert_eq!(reply, "NF\r\nHD\r\nEX\r\nHD\r\n");
    let stats = exchange(addr, "stats\r\n").await;
    assert_eq!(stat_value(&stats, "cas_misses"), 1);
    assert_eq!(stat_value(&stats, "cas_badval"), 1);
    assert_eq!(stat_value(&stats, "cas_hits"), 1);
    assert_eq!(stat_value(&stats, "cmd_set"), 4);
}

#[tokio::test]
async fn mg_with_a_ttl_updates_the_touch_counters() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "ms k 1\r\na\r\nmg k T30\r\nmg missing T30\r\nmg k\r\n",
    )
    .await;
    assert_eq!(reply, "HD\r\nHD\r\nEN\r\nHD\r\n");
    let stats = exchange(addr, "stats\r\n").await;
    // memcached counts a touching hit as a touch only and a touching miss as a get only.
    assert_eq!(stat_value(&stats, "cmd_touch"), 1);
    assert_eq!(stat_value(&stats, "touch_hits"), 1);
    assert_eq!(stat_value(&stats, "touch_misses"), 0);
    assert_eq!(stat_value(&stats, "cmd_get"), 2);
    assert_eq!(stat_value(&stats, "get_hits"), 1);
    assert_eq!(stat_value(&stats, "get_misses"), 1);
}
