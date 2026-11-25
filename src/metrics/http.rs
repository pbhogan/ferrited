//! Minimal HTTP/1.1 for the metrics endpoint. Only `GET` and `HEAD` of `/metrics` succeed;
//! request bodies are never read.

/// Longest request head accepted, including the terminating blank line.
pub(crate) const MAX_HEAD: usize = 8 * 1024;

/// The media type of the Prometheus text exposition format.
pub(crate) const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Parsed<'a> {
    Incomplete,
    TooLarge,
    /// Not an HTTP/1 request.
    Bad,
    /// A request for `path`, without its query.
    Request {
        method: &'a str,
        path: &'a str,
    },
}

/// Parses a request head from the bytes received so far. It ends at the first `\r\n\r\n`.
pub(crate) fn parse(buffer: &[u8]) -> Parsed<'_> {
    let Some(end) = memchr::memmem::find(buffer, b"\r\n\r\n") else {
        return match buffer.len() >= MAX_HEAD {
            true => Parsed::TooLarge,
            false => Parsed::Incomplete,
        };
    };
    if end + 4 > MAX_HEAD {
        return Parsed::TooLarge;
    }
    let head = &buffer[..end];
    let line_end = memchr::memmem::find(head, b"\r\n").unwrap_or(head.len());
    let Ok(line) = std::str::from_utf8(&head[..line_end]) else {
        return Parsed::Bad;
    };
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Parsed::Bad;
    };
    let token = !method.is_empty() && method.bytes().all(|byte| byte.is_ascii_alphabetic());
    let http1 = version.len() == 8 && version.starts_with("HTTP/1.");
    if !token || !http1 || !target.starts_with('/') {
        return Parsed::Bad;
    }
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    Parsed::Request { method, path }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Route {
    /// `head` for a `HEAD` request, which gets no body.
    Metrics {
        head: bool,
    },
    NotFound,
    MethodNotAllowed,
    BadRequest,
    TooLarge,
}

/// Decides how to answer a parsed request; `None` while it is incomplete.
pub(crate) fn route(parsed: &Parsed<'_>) -> Option<Route> {
    Some(match parsed {
        Parsed::Incomplete => return None,
        Parsed::TooLarge => Route::TooLarge,
        Parsed::Bad => Route::BadRequest,
        Parsed::Request { path, .. } if *path != "/metrics" => Route::NotFound,
        Parsed::Request { method: "GET", .. } => Route::Metrics { head: false },
        Parsed::Request { method: "HEAD", .. } => Route::Metrics { head: true },
        Parsed::Request { .. } => Route::MethodNotAllowed,
    })
}

/// The whole response to `route`. `metrics` is called only for a metrics request.
pub(crate) fn respond(route: &Route, metrics: impl FnOnce() -> String) -> Vec<u8> {
    match route {
        Route::Metrics { head } => {
            let body = metrics();
            response("200 OK", CONTENT_TYPE, "", &body, *head)
        }
        Route::NotFound => plain("404 Not Found", "", "Not found; metrics are at /metrics\n"),
        Route::MethodNotAllowed => plain(
            "405 Method Not Allowed",
            "Allow: GET, HEAD\r\n",
            "Only GET and HEAD are allowed\n",
        ),
        Route::BadRequest => plain("400 Bad Request", "", "Bad request\n"),
        Route::TooLarge => plain(
            "431 Request Header Fields Too Large",
            "",
            "Request head too large\n",
        ),
    }
}

/// The response to a connection over the connection limit.
pub(crate) fn busy() -> Vec<u8> {
    plain(
        "503 Service Unavailable",
        "",
        "Too many metrics requests at once\n",
    )
}

pub(crate) fn timed_out() -> Vec<u8> {
    plain("408 Request Timeout", "", "Request not received in time\n")
}

pub(crate) fn internal_error() -> Vec<u8> {
    plain(
        "500 Internal Server Error",
        "",
        "Cannot render the metrics\n",
    )
}

/// A response with a short text body. `headers` are extra header lines, each ending in `\r\n`.
fn plain(status: &str, headers: &str, body: &str) -> Vec<u8> {
    response(status, "text/plain; charset=utf-8", headers, body, false)
}

/// With `head`, the body is left out but its length is still sent.
fn response(status: &str, content_type: &str, headers: &str, body: &str, head: bool) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    if !head {
        out.extend_from_slice(body.as_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn routed(request: &[u8]) -> Option<Route> {
        route(&parse(request))
    }

    #[test]
    fn get_and_head_of_metrics_succeed() {
        let get = b"GET /metrics HTTP/1.1\r\nHost: node:9150\r\nAccept: */*\r\n\r\n";
        assert_eq!(
            parse(get),
            Parsed::Request {
                method: "GET",
                path: "/metrics"
            }
        );
        assert_eq!(routed(get), Some(Route::Metrics { head: false }));
        let head = b"HEAD /metrics HTTP/1.0\r\n\r\n";
        assert_eq!(routed(head), Some(Route::Metrics { head: true }));
        // Prometheus can add query parameters; they are ignored.
        let query = b"GET /metrics?module=x HTTP/1.1\r\n\r\n";
        assert_eq!(routed(query), Some(Route::Metrics { head: false }));
    }

    #[test]
    fn other_paths_are_not_found_and_other_methods_not_allowed() {
        assert_eq!(routed(b"GET / HTTP/1.1\r\n\r\n"), Some(Route::NotFound));
        assert_eq!(
            routed(b"GET /metrics/x HTTP/1.1\r\n\r\n"),
            Some(Route::NotFound)
        );
        assert_eq!(
            routed(b"POST /metrics HTTP/1.1\r\nContent-Length: 2\r\n\r\nhi"),
            Some(Route::MethodNotAllowed)
        );
        assert_eq!(
            routed(b"DELETE /other HTTP/1.1\r\n\r\n"),
            Some(Route::NotFound)
        );
    }

    #[test]
    fn malformed_request_lines_are_bad_requests() {
        for bad in [
            &b"stats\r\n\r\n"[..],
            b"GET /metrics\r\n\r\n",
            b"GET /metrics HTTP/2.0\r\n\r\n",
            b"GET  /metrics HTTP/1.1\r\n\r\n",
            b"GET metrics HTTP/1.1\r\n\r\n",
            b"GET /metrics HTTP/1.1 extra\r\n\r\n",
            b"G=T /metrics HTTP/1.1\r\n\r\n",
            b"GET /metr\xffics HTTP/1.1\r\n\r\n",
            b"\r\n\r\n",
        ] {
            assert_eq!(routed(bad), Some(Route::BadRequest), "{bad:?}");
        }
    }

    #[test]
    fn a_head_without_its_blank_line_is_incomplete_until_it_is_too_large() {
        assert_eq!(parse(b""), Parsed::Incomplete);
        assert_eq!(parse(b"GET /metrics HTTP/1.1\r\n"), Parsed::Incomplete);
        assert_eq!(
            parse(b"GET /metrics HTTP/1.1\r\nHost: x\r\n"),
            Parsed::Incomplete
        );
        // Bare line feeds do not end the head.
        assert_eq!(parse(b"GET /metrics HTTP/1.1\n\n"), Parsed::Incomplete);
        assert_eq!(routed(b"GET /metrics HTTP/1.1\r\n"), None);
        let mut long = b"GET /metrics HTTP/1.1\r\nX: ".to_vec();
        long.resize(MAX_HEAD - 1, b'a');
        assert_eq!(parse(&long), Parsed::Incomplete);
        long.push(b'a');
        assert_eq!(parse(&long), Parsed::TooLarge);
        assert_eq!(routed(&long), Some(Route::TooLarge));
        // A head that ends just past the limit is too large too.
        let mut over = long[..MAX_HEAD - 3].to_vec();
        over.extend_from_slice(b"\r\n\r\n");
        assert_eq!(parse(&over), Parsed::TooLarge);
        let mut fits = long[..MAX_HEAD - 4].to_vec();
        fits.extend_from_slice(b"\r\n\r\n");
        assert_eq!(
            parse(&fits),
            Parsed::Request {
                method: "GET",
                path: "/metrics"
            }
        );
    }

    #[test]
    fn responses_carry_length_type_and_close() {
        let ok =
            String::from_utf8(respond(&Route::Metrics { head: false }, || "a 1\n".into())).unwrap();
        assert_eq!(
            ok,
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
             Content-Length: 4\r\nConnection: close\r\n\r\na 1\n"
        );
        let head =
            String::from_utf8(respond(&Route::Metrics { head: true }, || "a 1\n".into())).unwrap();
        assert!(head.contains("Content-Length: 4\r\n"), "{head}");
        assert!(head.ends_with("\r\n\r\n"), "{head}");
        let not_allowed = String::from_utf8(respond(&Route::MethodNotAllowed, || {
            unreachable!("only a metrics request renders")
        }))
        .unwrap();
        assert!(not_allowed.starts_with("HTTP/1.1 405 "), "{not_allowed}");
        assert!(
            not_allowed.contains("\r\nAllow: GET, HEAD\r\n"),
            "{not_allowed}"
        );
        for (route, status) in [
            (Route::NotFound, "404"),
            (Route::BadRequest, "400"),
            (Route::TooLarge, "431"),
        ] {
            let text = String::from_utf8(respond(&route, String::new)).unwrap();
            assert!(text.starts_with(&format!("HTTP/1.1 {status} ")), "{text}");
            let (head, body) = text.split_once("\r\n\r\n").unwrap();
            assert!(head.contains(&format!("Content-Length: {}\r\n", body.len())));
            assert!(head.ends_with("Connection: close"), "{text}");
        }
        for (response, status) in [
            (busy(), "503"),
            (timed_out(), "408"),
            (internal_error(), "500"),
        ] {
            let text = String::from_utf8(response).unwrap();
            assert!(text.starts_with(&format!("HTTP/1.1 {status} ")), "{text}");
        }
    }
}
