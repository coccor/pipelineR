//! Pagination logic for the REST source connector.

use serde_json::Value as JsonValue;

use crate::config::PaginationConfig;
use crate::response::extract_path;

/// The result of checking for a next page.
#[derive(Debug)]
pub enum NextPage {
    /// There is another page to fetch.
    Continue(PageState),
    /// There are no more pages.
    Done,
}

/// State needed to fetch the next page.
#[derive(Debug, Clone)]
pub enum PageState {
    /// Cursor-based: send this cursor value as a query parameter.
    Cursor(String),
    /// Offset-based: the next offset to use.
    Offset(u64),
    /// Page-number-based: the next page number.
    PageNumber(u64),
    /// Link-header-based: the full URL of the next page.
    LinkUrl(String),
}

/// Context for determining pagination state after a response.
pub struct PageContext<'a> {
    /// The pagination configuration.
    pub config: &'a PaginationConfig,
    /// The response body.
    pub body: &'a JsonValue,
    /// The response headers.
    pub headers: &'a reqwest::header::HeaderMap,
    /// Dot-notation path to the cursor value in the response.
    pub cursor_path: Option<&'a str>,
    /// Dot-notation path to the total count in the response.
    pub total_path: Option<&'a str>,
    /// The current offset (for offset pagination).
    pub current_offset: u64,
    /// Number of records in this page.
    pub records_count: u64,
    /// The current page number (for page-number pagination).
    pub current_page: u64,
}

/// Determine the next page state from the current response.
///
/// Uses the pagination config, response body, response headers, and count of records
/// received on this page to decide whether there are more pages.
pub fn next_page(ctx: &PageContext<'_>) -> NextPage {
    let PageContext {
        config,
        body,
        headers,
        cursor_path,
        total_path,
        current_offset,
        records_count,
        current_page,
    } = ctx;
    match config {
        PaginationConfig::Cursor {
            cursor_field: _,
            cursor_param: _,
        } => {
            // Look for the next cursor in the response body.
            let path = cursor_path.unwrap_or("");
            if let Some(cursor_val) = extract_path(body, path) {
                if let Some(s) = cursor_val.as_str() {
                    if !s.is_empty() {
                        return NextPage::Continue(PageState::Cursor(s.to_string()));
                    }
                }
                // Also handle numeric cursors.
                if let Some(n) = cursor_val.as_i64() {
                    return NextPage::Continue(PageState::Cursor(n.to_string()));
                }
            }
            NextPage::Done
        }
        PaginationConfig::Offset {
            offset_param: _,
            limit_param: _,
            limit,
        } => {
            // Check if we have a total count.
            if let Some(tp) = total_path {
                if let Some(total_val) = extract_path(body, tp) {
                    if let Some(total) = total_val.as_u64() {
                        let next_offset = *current_offset + *records_count;
                        if next_offset < total {
                            return NextPage::Continue(PageState::Offset(next_offset));
                        }
                        return NextPage::Done;
                    }
                }
            }
            // Fallback: if we got a full page of records, assume there are more.
            if *records_count >= *limit {
                return NextPage::Continue(PageState::Offset(*current_offset + *records_count));
            }
            NextPage::Done
        }
        PaginationConfig::PageNumber {
            page_param: _,
            page_size_param: _,
            page_size,
        } => {
            // If we got fewer records than the page size, we're done.
            let effective_page_size = page_size.unwrap_or(100);
            if *records_count < effective_page_size {
                return NextPage::Done;
            }
            NextPage::Continue(PageState::PageNumber(*current_page + 1))
        }
        PaginationConfig::LinkHeader => {
            // Parse the Link header for rel="next".
            if let Some(link_val) = headers.get("link").or_else(|| headers.get("Link")) {
                if let Ok(link_str) = link_val.to_str() {
                    if let Some(next_url) = parse_link_header_next(link_str) {
                        return NextPage::Continue(PageState::LinkUrl(next_url));
                    }
                }
            }
            NextPage::Done
        }
    }
}

/// Apply the current page state as query parameters on a URL.
///
/// Returns the modified URL string, or `None` if the page state is a `LinkUrl`
/// (in which case the caller should use the URL directly).
pub fn apply_page_state(
    url: &mut url::Url,
    config: &PaginationConfig,
    state: &PageState,
) {
    match (config, state) {
        (PaginationConfig::Cursor { cursor_param, .. }, PageState::Cursor(val)) => {
            url.query_pairs_mut().append_pair(cursor_param, val);
        }
        (
            PaginationConfig::Offset {
                offset_param,
                limit_param,
                limit,
            },
            PageState::Offset(offset),
        ) => {
            url.query_pairs_mut()
                .append_pair(offset_param, &offset.to_string())
                .append_pair(limit_param, &limit.to_string());
        }
        (
            PaginationConfig::PageNumber {
                page_param,
                page_size_param,
                page_size,
            },
            PageState::PageNumber(page),
        ) => {
            url.query_pairs_mut()
                .append_pair(page_param, &page.to_string());
            if let (Some(param), Some(size)) = (page_size_param, page_size) {
                url.query_pairs_mut()
                    .append_pair(param, &size.to_string());
            }
        }
        _ => {}
    }
}

/// Parse a Link header value and extract the URL with `rel="next"`.
fn parse_link_header_next(header: &str) -> Option<String> {
    for part in header.split(',') {
        let part = part.trim();
        // Each part looks like: <URL>; rel="next"
        if part.contains("rel=\"next\"") || part.contains("rel='next'") {
            // Extract URL between < and >.
            if let Some(start) = part.find('<') {
                if let Some(end) = part.find('>') {
                    return Some(part[start + 1..end].to_string());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_link_header_finds_next() {
        let header = r#"<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last""#;
        let result = parse_link_header_next(header);
        assert_eq!(
            result,
            Some("https://api.example.com/items?page=2".to_string())
        );
    }

    #[test]
    fn parse_link_header_no_next() {
        let header = r#"<https://api.example.com/items?page=1>; rel="prev""#;
        let result = parse_link_header_next(header);
        assert_eq!(result, None);
    }

    #[test]
    fn offset_pagination_done_when_below_limit() {
        let config = PaginationConfig::Offset {
            offset_param: "offset".to_string(),
            limit_param: "limit".to_string(),
            limit: 100,
        };
        let body = serde_json::json!({});
        let headers = reqwest::header::HeaderMap::new();
        let ctx = PageContext {
            config: &config,
            body: &body,
            headers: &headers,
            cursor_path: None,
            total_path: None,
            current_offset: 0,
            records_count: 50,
            current_page: 0,
        };
        match next_page(&ctx) {
            NextPage::Done => {}
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn offset_pagination_continues_when_full_page() {
        let config = PaginationConfig::Offset {
            offset_param: "offset".to_string(),
            limit_param: "limit".to_string(),
            limit: 100,
        };
        let body = serde_json::json!({});
        let headers = reqwest::header::HeaderMap::new();
        let ctx = PageContext {
            config: &config,
            body: &body,
            headers: &headers,
            cursor_path: None,
            total_path: None,
            current_offset: 0,
            records_count: 100,
            current_page: 0,
        };
        match next_page(&ctx) {
            NextPage::Continue(PageState::Offset(100)) => {}
            other => panic!("expected Continue(Offset(100)), got {other:?}"),
        }
    }
}
