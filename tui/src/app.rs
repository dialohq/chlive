//! The TUI's state, without the terminal: the query input, and the live result as the
//! subscription's updates leave it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chlive_client::{Change, Column, Kind, Row, Rows, Status, SubscriptionStats, Update};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::Style;
use ratatui::text::Span;
use serde_json::Value;

/// How long a row stays highlighted after a diff inserted or changed it.
pub const HIGHLIGHT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Query,
    Table,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    None,
    /// Subscribe to this query (replacing the current one).
    Run(String),
    Quit,
}

pub struct App {
    /// The server, for the status line.
    pub url: String,
    pub input: String,
    /// In chars.
    pub cursor: usize,
    pub focus: Focus,
    /// The query being shown, once one ran.
    pub running: Option<String>,
    pub columns: Arc<[Column]>,
    pub rows: Rows<Row>,
    pub status: Status,
    pub disconnected: bool,
    pub stats: SubscriptionStats,
    /// Rows the recent diffs inserted or changed (the same `Arc`s as in `rows`), and when.
    pub highlights: Vec<(Arc<Row>, Instant)>,
    /// The first row shown.
    pub scroll: usize,
    /// How many rows the table shows, as last drawn.
    pub page: usize,
}

impl App {
    pub fn new(url: impl Into<String>, sql: impl Into<String>) -> Self {
        let input: String = sql.into();
        App {
            url: url.into(),
            cursor: input.chars().count(),
            input,
            focus: Focus::Query,
            running: None,
            columns: Arc::from(Vec::new()),
            rows: Arc::new(Vec::new()),
            status: Status::Loading,
            disconnected: false,
            stats: SubscriptionStats::default(),
            highlights: Vec::new(),
            scroll: 0,
            page: 10,
        }
    }

    /// A new query starts: forget the last one's result.
    pub fn run(&mut self, sql: &str) {
        self.running = Some(sql.to_string());
        self.columns = Arc::from(Vec::new());
        self.rows = Arc::new(Vec::new());
        self.status = Status::Loading;
        self.disconnected = false;
        self.stats = SubscriptionStats::default();
        self.highlights.clear();
        self.scroll = 0;
    }

    pub fn on_update(&mut self, u: Update<Row>) {
        self.on_update_at(u, Instant::now())
    }

    pub fn on_update_at(&mut self, u: Update<Row>, now: Instant) {
        self.disconnected = u.kind == Kind::Disconnected;
        if u.kind == Kind::Snapshot {
            self.highlights.clear();
        }
        for c in &u.changes {
            match c {
                Change::Insert { row, .. } | Change::Update { row, .. } => {
                    self.highlights.push((row.clone(), now))
                }
                Change::Delete { .. } => {}
            }
        }
        if !u.columns.is_empty() {
            self.columns = u.columns;
        }
        self.rows = u.rows;
        self.status = u.status;
        self.stats = u.stats;
        self.highlights
            .retain(|(h, _)| self.rows.iter().any(|r| Arc::ptr_eq(r, h)));
        self.clamp_scroll();
    }

    /// Fade the highlights.
    pub fn tick(&mut self, now: Instant) {
        self.highlights
            .retain(|(_, at)| now.duration_since(*at) < HIGHLIGHT);
    }

    pub fn highlighted(&self, row: &Arc<Row>) -> bool {
        self.highlights.iter().any(|(h, _)| Arc::ptr_eq(h, row))
    }

    pub fn on_key(&mut self, key: KeyEvent) -> Action {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('c') if ctrl => return Action::Quit,
            KeyCode::Esc => return Action::Quit,
            KeyCode::Tab | KeyCode::BackTab => {
                self.focus = match self.focus {
                    Focus::Query => Focus::Table,
                    Focus::Table => Focus::Query,
                };
            }
            KeyCode::Enter => {
                let sql = self.input.trim();
                if !sql.is_empty() {
                    return Action::Run(sql.to_string());
                }
            }
            KeyCode::Up => self.scroll_by(-1),
            KeyCode::Down => self.scroll_by(1),
            KeyCode::PageUp => self.scroll_by(-(self.page.max(1) as isize)),
            KeyCode::PageDown => self.scroll_by(self.page.max(1) as isize),
            _ if self.focus == Focus::Table => match key.code {
                KeyCode::Char('k') => self.scroll_by(-1),
                KeyCode::Char('j') => self.scroll_by(1),
                KeyCode::Home | KeyCode::Char('g') => self.scroll = 0,
                KeyCode::End | KeyCode::Char('G') => self.scroll = usize::MAX,
                _ => {}
            },
            _ => self.edit(key),
        }
        self.clamp_scroll();
        Action::None
    }

    /// Editing the query, with the usual shortcuts of readline and macOS text fields. Option
    /// arrives as Alt (the terminal sends it as Meta), and Cmd as Super where the terminal reports
    /// it (the kitty keyboard protocol); terminals that don't send Cmd+Delete as Ctrl-U.
    fn edit(&mut self, key: KeyEvent) {
        let m = key.modifiers;
        let ctrl = m.contains(KeyModifiers::CONTROL);
        let alt = m.intersects(KeyModifiers::ALT | KeyModifiers::META);
        let cmd = m.contains(KeyModifiers::SUPER);
        let len = self.input.chars().count();
        let at = self.cursor;
        match key.code {
            // moving
            KeyCode::Char('a') if ctrl => self.cursor = 0,
            KeyCode::Char('e') if ctrl => self.cursor = len,
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = len,
            KeyCode::Left if cmd => self.cursor = 0,
            KeyCode::Right if cmd => self.cursor = len,
            KeyCode::Char('b') if alt => self.cursor = self.word_start(),
            KeyCode::Char('f') if alt => self.cursor = self.word_end(),
            KeyCode::Left if alt || ctrl => self.cursor = self.word_start(),
            KeyCode::Right if alt || ctrl => self.cursor = self.word_end(),
            KeyCode::Char('b') if ctrl => self.cursor = at.saturating_sub(1),
            KeyCode::Char('f') if ctrl => self.cursor = (at + 1).min(len),
            KeyCode::Left => self.cursor = at.saturating_sub(1),
            KeyCode::Right => self.cursor = (at + 1).min(len),
            // deleting
            KeyCode::Backspace if cmd => self.delete(0, at),
            KeyCode::Char('u') if ctrl => self.delete(0, at),
            KeyCode::Delete if cmd => self.delete(at, len),
            KeyCode::Char('k') if ctrl => self.delete(at, len),
            KeyCode::Backspace if alt || ctrl => self.delete(self.word_start(), at),
            KeyCode::Char('w') if ctrl => self.delete(self.space_word_start(), at),
            KeyCode::Delete if alt => self.delete(at, self.word_end()),
            KeyCode::Char('d') if alt => self.delete(at, self.word_end()),
            KeyCode::Backspace => self.delete(at.saturating_sub(1), at),
            KeyCode::Char('h') if ctrl => self.delete(at.saturating_sub(1), at),
            KeyCode::Delete => self.delete(at, (at + 1).min(len)),
            KeyCode::Char('d') if ctrl => self.delete(at, (at + 1).min(len)),
            // typing
            KeyCode::Char(c) if !ctrl && !alt && !cmd => {
                let b = self.byte(at);
                self.input.insert(b, c);
                self.cursor += 1;
            }
            _ => {}
        }
    }

    /// Delete the chars `from..to`, leaving the cursor at `from`.
    fn delete(&mut self, from: usize, to: usize) {
        let (a, b) = (self.byte(from), self.byte(to));
        self.input.replace_range(a..b, "");
        self.cursor = from;
    }

    /// Where the word before the cursor starts (words are letters, digits and `_`), as Option+←.
    fn word_start(&self) -> usize {
        let chars: Vec<char> = self.input.chars().collect();
        let mut i = self.cursor;
        while i > 0 && !is_word(chars[i - 1]) {
            i -= 1;
        }
        while i > 0 && is_word(chars[i - 1]) {
            i -= 1;
        }
        i
    }

    /// Where the word after the cursor ends, as Option+→.
    fn word_end(&self) -> usize {
        let chars: Vec<char> = self.input.chars().collect();
        let mut i = self.cursor;
        while i < chars.len() && !is_word(chars[i]) {
            i += 1;
        }
        while i < chars.len() && is_word(chars[i]) {
            i += 1;
        }
        i
    }

    /// Where the whitespace-separated word before the cursor starts, for Ctrl-W.
    fn space_word_start(&self) -> usize {
        let chars: Vec<char> = self.input.chars().collect();
        let mut i = self.cursor;
        while i > 0 && chars[i - 1].is_whitespace() {
            i -= 1;
        }
        while i > 0 && !chars[i - 1].is_whitespace() {
            i -= 1;
        }
        i
    }

    fn byte(&self, char_index: usize) -> usize {
        self.input
            .char_indices()
            .nth(char_index)
            .map_or(self.input.len(), |(i, _)| i)
    }

    fn scroll_by(&mut self, by: isize) {
        self.scroll = self.scroll.saturating_add_signed(by);
        self.clamp_scroll();
    }

    pub fn clamp_scroll(&mut self) {
        self.scroll = self.scroll.min(self.rows.len().saturating_sub(self.page));
    }
}

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// A value as a table cell: strings as they are, NULL dimmed, anything else as JSON.
pub fn cell(v: Option<&Value>) -> Span<'static> {
    match v {
        None | Some(Value::Null) => Span::styled("NULL", Style::new().dim()),
        Some(Value::String(s)) => Span::raw(s.clone()),
        Some(v) => Span::raw(v.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chlive_client::LiveError;
    use serde_json::json;

    fn row(v: Value) -> Arc<Row> {
        Arc::new(v.as_object().unwrap().clone())
    }

    fn columns() -> Arc<[Column]> {
        Arc::from(vec![
            Column {
                name: "u".into(),
                ty: "String".into(),
            },
            Column {
                name: "n".into(),
                ty: "UInt32".into(),
            },
        ])
    }

    fn update(
        kind: Kind,
        status: Status,
        rows: Vec<Arc<Row>>,
        changes: Vec<Change<Row>>,
    ) -> Update<Row> {
        Update {
            kind,
            status,
            rows: Arc::new(rows),
            changes,
            columns: if kind == Kind::Subscribed {
                Arc::from(Vec::new())
            } else {
                columns()
            },
            at: None,
            stats: SubscriptionStats::default(),
        }
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn diffs_update_the_rows_and_highlight_what_they_touched() {
        let mut app = App::new("ws://x/live", "select u, n from t order by n desc");
        app.run("select u, n from t order by n desc");
        let t0 = Instant::now();
        app.on_update_at(
            update(Kind::Subscribed, Status::Loading, vec![], vec![]),
            t0,
        );
        assert!(app.columns.is_empty());

        let (a, b) = (
            row(json!({"u": "a", "n": 2})),
            row(json!({"u": "b", "n": 1})),
        );
        app.on_update_at(
            update(
                Kind::Snapshot,
                Status::Live,
                vec![a.clone(), b.clone()],
                vec![],
            ),
            t0,
        );
        assert_eq!(app.columns.len(), 2);
        assert_eq!(app.rows.len(), 2);
        assert!(app.highlights.is_empty());

        // c goes in at the top and pushes b out
        let c = row(json!({"u": "c", "n": 3}));
        app.on_update_at(
            update(
                Kind::Diff,
                Status::Live,
                vec![c.clone(), a.clone()],
                vec![
                    Change::Insert {
                        row: c.clone(),
                        index: Some(0),
                        id: None,
                    },
                    Change::Delete {
                        index: 2,
                        row: b.clone(),
                    },
                ],
            ),
            t0,
        );
        assert!(app.highlighted(&app.rows[0].clone()));
        assert!(!app.highlighted(&a));

        // a changes in place
        let a2 = row(json!({"u": "a", "n": 2}));
        app.on_update_at(
            update(
                Kind::Diff,
                Status::Live,
                vec![c.clone(), a2.clone()],
                vec![Change::Update {
                    id: "[\"a\"]".into(),
                    row: a2.clone(),
                    index: None,
                }],
            ),
            t0 + Duration::from_millis(500),
        );
        assert!(app.highlighted(&c) && app.highlighted(&a2));

        app.tick(t0 + Duration::from_millis(1200));
        assert!(!app.highlighted(&c) && app.highlighted(&a2));
        app.tick(t0 + Duration::from_millis(1600));
        assert!(app.highlights.is_empty());
    }

    #[test]
    fn an_error_keeps_the_rows() {
        let mut app = App::new("ws://x/live", "");
        app.run("select u from t");
        let a = row(json!({"u": "a", "n": 1}));
        app.on_update(update(
            Kind::Snapshot,
            Status::Live,
            vec![a.clone()],
            vec![],
        ));
        let e = LiveError {
            code: "STREAM_FAILED".into(),
            message: "gone".into(),
        };
        app.on_update(update(
            Kind::Error,
            Status::Error(e.clone()),
            vec![a],
            vec![],
        ));
        assert_eq!(app.status, Status::Error(e));
        assert_eq!(app.rows.len(), 1);

        app.run("select n from t");
        assert!(app.rows.is_empty() && app.columns.is_empty());
        assert_eq!(app.status, Status::Loading);
    }

    #[test]
    fn editing_running_and_quitting() {
        let mut app = App::new("ws://x/live", "selct");
        app.on_key(key(KeyCode::Left));
        app.on_key(key(KeyCode::Left));
        app.on_key(key(KeyCode::Char('e')));
        assert_eq!((app.input.as_str(), app.cursor), ("select", 4));
        app.on_key(key(KeyCode::End));
        for c in " 1 → ".chars() {
            app.on_key(key(KeyCode::Char(c)));
        }
        app.on_key(key(KeyCode::Backspace));
        app.on_key(key(KeyCode::Backspace));
        assert_eq!(app.input, "select 1 ");
        app.on_key(key(KeyCode::Home));
        app.on_key(key(KeyCode::Delete));
        assert_eq!(app.input, "elect 1 ");
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Action::Run("elect 1".into())
        );
        app.on_key(key(KeyCode::End));
        app.on_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        assert_eq!(app.input, "");
        assert_eq!(app.on_key(key(KeyCode::Enter)), Action::None);
        assert_eq!(app.on_key(key(KeyCode::Esc)), Action::Quit);
        assert_eq!(
            app.on_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Action::Quit
        );
    }

    #[test]
    fn readline_and_macos_shortcuts() {
        use KeyModifiers as M;
        // the input, then keys, then the input with `|` at the cursor
        type Case = (&'static str, &'static [(KeyCode, M)], &'static str);
        let cases: &[Case] = &[
            ("select 1", &[(KeyCode::Char('a'), M::CONTROL)], "|select 1"),
            (
                "select 1",
                &[(KeyCode::Home, M::NONE), (KeyCode::Char('e'), M::CONTROL)],
                "select 1|",
            ),
            ("select 1", &[(KeyCode::Char('b'), M::CONTROL)], "select |1"),
            (
                "select 1",
                &[(KeyCode::Home, M::NONE), (KeyCode::Char('f'), M::CONTROL)],
                "s|elect 1",
            ),
            (
                "select a.b_c",
                &[(KeyCode::Char('b'), M::ALT)],
                "select a.|b_c",
            ),
            (
                "select a.b_c",
                &[(KeyCode::Left, M::ALT), (KeyCode::Left, M::ALT)],
                "select |a.b_c",
            ),
            (
                "select a.b_c",
                &[(KeyCode::Home, M::NONE), (KeyCode::Char('f'), M::ALT)],
                "select| a.b_c",
            ),
            (
                "select a.b_c",
                &[(KeyCode::Home, M::NONE), (KeyCode::Right, M::CONTROL)],
                "select| a.b_c",
            ),
            ("select 1", &[(KeyCode::Left, M::SUPER)], "|select 1"),
            (
                "select 1",
                &[(KeyCode::Home, M::NONE), (KeyCode::Right, M::SUPER)],
                "select 1|",
            ),
            (
                "select a.b_c",
                &[(KeyCode::Backspace, M::ALT)],
                "select a.|",
            ),
            (
                "select a.b_c",
                &[(KeyCode::Char('w'), M::CONTROL)],
                "select |",
            ),
            (
                "select a, b",
                &[(KeyCode::Left, M::ALT), (KeyCode::Backspace, M::SUPER)],
                "|b",
            ),
            (
                "select a, b",
                &[(KeyCode::Left, M::ALT), (KeyCode::Char('u'), M::CONTROL)],
                "|b",
            ),
            (
                "select a, b",
                &[(KeyCode::Left, M::ALT), (KeyCode::Char('k'), M::CONTROL)],
                "select a, |",
            ),
            (
                "select a, b",
                &[(KeyCode::Home, M::NONE), (KeyCode::Delete, M::SUPER)],
                "|",
            ),
            (
                "select a, b",
                &[(KeyCode::Home, M::NONE), (KeyCode::Char('d'), M::ALT)],
                "| a, b",
            ),
            (
                "select a, b",
                &[(KeyCode::Home, M::NONE), (KeyCode::Char('d'), M::CONTROL)],
                "|elect a, b",
            ),
            (
                "select a, b",
                &[(KeyCode::Char('h'), M::CONTROL)],
                "select a, |",
            ),
            (
                "sélect",
                &[
                    (KeyCode::Left, M::ALT),
                    (KeyCode::Right, M::NONE),
                    (KeyCode::Delete, M::NONE),
                ],
                "s|lect",
            ),
            // Option+letter without Meta isn't typed as the letter
            ("x", &[(KeyCode::Char('q'), M::ALT)], "x|"),
            ("", &[(KeyCode::Char('X'), M::SHIFT)], "X|"),
        ];
        for (input, keys, want) in cases {
            let mut app = App::new("ws://x/live", *input);
            for &(code, m) in keys.iter() {
                assert_eq!(app.on_key(KeyEvent::new(code, m)), Action::None);
            }
            let mut got = app.input.clone();
            got.insert(app.byte(app.cursor), '|');
            assert_eq!(&got, want, "{input:?} {keys:?}");
        }
    }

    #[test]
    fn the_table_scrolls_within_its_rows() {
        let mut app = App::new("ws://x/live", "");
        app.page = 3;
        let rows = (0..5).map(|n| row(json!({"u": "x", "n": n}))).collect();
        app.on_update(update(Kind::Snapshot, Status::Live, rows, vec![]));
        app.on_key(key(KeyCode::Tab));
        assert_eq!(app.focus, Focus::Table);
        app.on_key(key(KeyCode::Char('j')));
        assert_eq!(app.scroll, 1);
        app.on_key(key(KeyCode::PageDown));
        assert_eq!(app.scroll, 2);
        app.on_key(key(KeyCode::Char('g')));
        assert_eq!(app.scroll, 0);
        app.on_key(key(KeyCode::End));
        assert_eq!(app.scroll, 2);
        app.on_key(key(KeyCode::PageUp));
        assert_eq!(app.scroll, 0);
        // typing in the table doesn't edit the query
        app.on_key(key(KeyCode::Char('x')));
        assert_eq!(app.input, "");
        app.on_key(key(KeyCode::Tab));
        assert_eq!(app.focus, Focus::Query);
    }

    #[test]
    fn cells() {
        let text = |v: Option<&Value>| cell(v).content.into_owned();
        assert_eq!(text(Some(&json!("a b"))), "a b");
        assert_eq!(text(Some(&json!(42))), "42");
        assert_eq!(text(Some(&json!(1.5))), "1.5");
        assert_eq!(text(Some(&json!(true))), "true");
        assert_eq!(text(Some(&json!(["x", 1]))), "[\"x\",1]");
        assert_eq!(text(Some(&Value::Null)), "NULL");
        assert_eq!(text(None), "NULL");
        assert_eq!(cell(None).style, Style::new().dim());
    }
}
