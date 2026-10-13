//! Drawing the [`App`]: the query input, a status line, the result table and the keys.

use std::time::SystemTime;

use chlive_client::Status;
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::style::{Color, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Paragraph, Row, Table, Wrap};
use ratatui::Frame;

use crate::app::{cell, App, Focus};

/// The widest a column gets (in chars), so one long value doesn't crowd out the others.
const MAX_WIDTH: usize = 40;

pub fn draw(frame: &mut Frame, app: &mut App) {
    // the status wraps, so a long error can be read whole
    let status_text = status_line(app);
    let width = frame.area().width.max(1) as usize;
    let status_height = status_text.width().div_ceil(width).clamp(1, 4) as u16;
    let [query, status, table, keys] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(status_height),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());
    draw_query(frame, app, query);
    frame.render_widget(
        Paragraph::new(status_text).wrap(Wrap { trim: false }),
        status,
    );
    draw_table(frame, app, table);
    frame.render_widget(
        Paragraph::new(
            " Enter run · Tab query/table · ↑↓ PgUp PgDn scroll · Ctrl-U clear · Esc quit".dim(),
        ),
        keys,
    );
}

fn border(focused: bool) -> Style {
    if focused {
        Style::new().fg(Color::Cyan)
    } else {
        Style::new().dark_gray()
    }
}

fn draw_query(frame: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Query;
    let block = Block::bordered()
        .title(" query ")
        .border_style(border(focused));
    let width = block.inner(area).width.max(1) as usize;
    // keep the cursor in view
    let offset = app.cursor.saturating_sub(width - 1);
    let text: String = app.input.chars().skip(offset).take(width).collect();
    let inner = block.inner(area);
    frame.render_widget(Paragraph::new(text).block(block), area);
    if focused {
        frame.set_cursor_position(Position::new(
            inner.x + (app.cursor - offset) as u16,
            inner.y,
        ));
    }
}

fn status_line(app: &App) -> Line<'static> {
    let mut spans = vec![Span::raw(" ")];
    if app.running.is_none() {
        spans.push("type a query and press Enter".dim());
    } else {
        match &app.status {
            Status::Error(e) => spans.push(format!("{}: {}", e.code, e.message).red().bold()),
            _ if app.disconnected => spans.push("disconnected; reconnecting".red()),
            Status::Loading => spans.push("loading…".yellow()),
            Status::Live => spans.push("live".green().bold()),
        }
        let s = &app.stats;
        spans.push(
            format!(
                " · {} rows · {} diffs (+{} −{})",
                app.rows.len(),
                s.diffs,
                s.inserts,
                s.deletes
            )
            .into(),
        );
        if let Some(ago) = s
            .last_message_at
            .and_then(|t| SystemTime::now().duration_since(t).ok())
        {
            spans.push(format!(" · last {:.1}s ago", ago.as_secs_f64()).into());
        }
    }
    spans.push(format!(" · {}", app.url).dim());
    Line::from(spans)
}

fn draw_table(frame: &mut Frame, app: &mut App, area: Rect) {
    let block = Block::bordered()
        .title(" result ")
        .border_style(border(app.focus == Focus::Table));
    // borders and the header
    app.page = (block.inner(area).height as usize).saturating_sub(1).max(1);
    app.clamp_scroll();
    let visible = &app.rows[app.scroll..(app.scroll + app.page).min(app.rows.len())];

    let number = |i: usize| format!("{}", i + 1);
    let mut widths = vec![number(app.rows.len().saturating_sub(1)).len().max(1)];
    let mut header = vec![Cell::from("#".dim())];
    for c in app.columns.iter() {
        let w = visible
            .iter()
            .map(|r| cell(r.get(&c.name)).width())
            .chain([c.name.chars().count() + 1 + c.ty.chars().count()])
            .max()
            .unwrap_or(0);
        widths.push(w.min(MAX_WIDTH));
        header.push(Cell::from(Line::from(vec![
            c.name.clone().bold(),
            " ".into(),
            c.ty.clone().dim(),
        ])));
    }
    let rows = visible.iter().enumerate().map(|(i, r)| {
        let mut cells = vec![Cell::from(number(app.scroll + i).dim())];
        cells.extend(app.columns.iter().map(|c| Cell::from(cell(r.get(&c.name)))));
        let row = Row::new(cells);
        if app.highlighted(r) {
            row.style(Style::new().bg(Color::Rgb(40, 70, 40)))
        } else {
            row
        }
    });
    let table = Table::new(rows, widths.iter().map(|&w| Constraint::Length(w as u16)))
        .header(Row::new(header))
        .column_spacing(2)
        .block(block);
    frame.render_widget(table, area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chlive_client::{Column, Kind, SubscriptionStats, Update};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn draws_the_query_the_status_and_the_typed_table() {
        let mut app = App::new("ws://127.0.0.1:1/live", "select u, n from t");
        app.run("select u, n from t");
        let row = |v: serde_json::Value| Arc::new(v.as_object().unwrap().clone());
        app.on_update(Update {
            kind: Kind::Snapshot,
            status: Status::Live,
            rows: Arc::new(vec![
                row(json!({"u": "alice", "n": 7})),
                row(json!({"u": null, "n": 8})),
            ]),
            changes: vec![],
            columns: Arc::from(vec![
                Column {
                    name: "u".into(),
                    ty: "Nullable(String)".into(),
                },
                Column {
                    name: "n".into(),
                    ty: "UInt32".into(),
                },
            ]),
            at: None,
            stats: SubscriptionStats {
                diffs: 3,
                inserts: 2,
                deletes: 1,
                ..Default::default()
            },
        });
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let screen: String = terminal
            .backend()
            .buffer()
            .content()
            .chunks(80)
            .map(|line| line.iter().map(|c| c.symbol()).collect::<String>() + "\n")
            .collect();
        for want in [
            "select u, n from t",
            "live · 2 rows · 3 diffs (+2 −1) · ws://127.0.0.1:1/live",
            "u Nullable(String)",
            "n UInt32",
            "alice",
            "NULL",
        ] {
            assert!(screen.contains(want), "{want:?} not in\n{screen}");
        }
        // 12 lines: query 3, status 1, keys 1, table borders 2 and header 1
        assert_eq!(app.page, 4);
    }
}
