//! A table's ClickHouse engine, as far as chlive cares: `MergeTree`, `ReplacingMergeTree` (with
//! its version and is-deleted columns) or anything else, which queries can't use. Replicated
//! forms are the same engines; their clones (see [`crate::snapshot`]) aren't replicated.

use std::fmt;

use crate::rules::quote_ident;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableEngine {
    MergeTree,
    /// `ReplacingMergeTree([ver [, is_deleted]])`: the ClickHouse columns.
    Replacing {
        ver: Option<String>,
        is_deleted: Option<String>,
    },
    /// Any other engine, as ClickHouse names it (`CollapsingMergeTree`, `View`, …).
    Other(String),
}

const REPLICATED: &str = "Replicated";

impl TableEngine {
    /// From `system.tables`: `engine` (`ReplicatedReplacingMergeTree`) and `engine_full`
    /// (`ReplicatedReplacingMergeTree('/p', '{replica}', v) ORDER BY k SETTINGS …`). Arguments
    /// it can't read make it [`TableEngine::Other`].
    pub fn from_system(engine: &str, engine_full: &str) -> Self {
        let other = || TableEngine::Other(engine.to_string());
        let replicated = Self::is_replicated(engine);
        let base = engine.strip_prefix(REPLICATED).unwrap_or(engine);
        if base != "MergeTree" && base != "ReplacingMergeTree" {
            return other();
        }
        let Some(mut args) = engine_full
            .strip_prefix(engine)
            .and_then(arguments)
            .map(|a| a.into_iter().map(Arg::read).collect::<Vec<_>>())
        else {
            return other();
        };
        if replicated {
            // the ZooKeeper path and replica name, when given
            let literals = args.iter().take_while(|a| matches!(a, Arg::Str)).count();
            args.drain(..literals);
        }
        let mut columns = Vec::new();
        for a in args {
            match a {
                Arg::Ident(name) => columns.push(name),
                Arg::Str | Arg::Other => return other(),
            }
        }
        match (base, columns.len()) {
            ("MergeTree", 0) => TableEngine::MergeTree,
            ("ReplacingMergeTree", 0..=2) => {
                let mut columns = columns.into_iter();
                TableEngine::Replacing {
                    ver: columns.next(),
                    is_deleted: columns.next(),
                }
            }
            _ => other(),
        }
    }

    /// Whether `system.tables`' `engine` is a replicated one.
    pub fn is_replicated(engine: &str) -> bool {
        engine.starts_with(REPLICATED)
    }

    /// The engine clause of a (not replicated) clone of the table; `None` for other engines.
    pub fn clone_engine(&self) -> Option<String> {
        match self {
            TableEngine::Other(_) => None,
            _ => Some(self.to_string()),
        }
    }
}

/// `MergeTree`, `ReplacingMergeTree(version, deleted)`: valid as an engine clause.
impl fmt::Display for TableEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableEngine::MergeTree => f.write_str("MergeTree"),
            TableEngine::Replacing { ver, is_deleted } => {
                let columns: Vec<String> = ver
                    .iter()
                    .chain(is_deleted)
                    .map(|c| quote_ident(c))
                    .collect();
                write!(f, "ReplacingMergeTree({})", columns.join(", "))
            }
            TableEngine::Other(name) => f.write_str(name),
        }
    }
}

enum Arg {
    Str,
    Ident(String),
    Other,
}

impl Arg {
    fn read(text: &str) -> Arg {
        let t = text.trim();
        if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
            Arg::Str
        } else if t.len() >= 2 && t.starts_with('`') && t.ends_with('`') {
            Arg::Ident(t[1..t.len() - 1].replace("\\`", "`").replace("\\\\", "\\"))
        } else if !t.is_empty()
            && !t.starts_with(|c: char| c.is_ascii_digit())
            && t.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            Arg::Ident(t.to_string())
        } else {
            Arg::Other
        }
    }
}

/// The arguments right after the engine's name: none without parentheses (`MergeTree ORDER BY
/// …`), else split at top-level commas. `None` when the parentheses don't close.
fn arguments(rest: &str) -> Option<Vec<&str>> {
    let Some(body) = rest.strip_prefix('(') else {
        return Some(Vec::new());
    };
    let (mut depth, mut quote, mut escaped) = (0usize, None::<char>, false);
    let (mut args, mut start) = (Vec::new(), 0);
    for (i, c) in body.char_indices() {
        if let Some(q) = quote {
            match c {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                _ if c == q => quote = None,
                _ => {}
            }
            continue;
        }
        match c {
            '\'' | '`' | '"' => quote = Some(c),
            '(' => depth += 1,
            ')' if depth == 0 => {
                let last = body[start..i].trim();
                if !(last.is_empty() && args.is_empty()) {
                    args.push(last);
                }
                return Some(args);
            }
            ')' => depth -= 1,
            ',' if depth == 0 => {
                args.push(body[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine(engine_full: &str) -> TableEngine {
        let name = engine_full.split(['(', ' ']).next().unwrap();
        TableEngine::from_system(name, engine_full)
    }

    fn replacing(ver: Option<&str>, is_deleted: Option<&str>) -> TableEngine {
        TableEngine::Replacing {
            ver: ver.map(Into::into),
            is_deleted: is_deleted.map(Into::into),
        }
    }

    #[test]
    fn merge_trees() {
        let settings = "ORDER BY ts SETTINGS index_granularity = 8192";
        assert_eq!(
            engine(&format!("MergeTree {settings}")),
            TableEngine::MergeTree
        );
        assert_eq!(
            engine(&format!("MergeTree() {settings}")),
            TableEngine::MergeTree
        );
        assert_eq!(
            engine("ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}') ORDER BY ts"),
            TableEngine::MergeTree
        );
        assert_eq!(
            engine("ReplicatedMergeTree ORDER BY ts"),
            TableEngine::MergeTree
        );
    }

    #[test]
    fn replacing_merge_trees() {
        assert_eq!(
            engine("ReplacingMergeTree ORDER BY k"),
            replacing(None, None)
        );
        assert_eq!(
            engine("ReplacingMergeTree(version) PARTITION BY toYYYYMM(d) ORDER BY k"),
            replacing(Some("version"), None)
        );
        assert_eq!(
            engine("ReplacingMergeTree(v, del) ORDER BY k SETTINGS index_granularity = 8192"),
            replacing(Some("v"), Some("del"))
        );
        assert_eq!(
            engine("ReplacingMergeTree(`my ver`, `is,deleted`) ORDER BY k"),
            replacing(Some("my ver"), Some("is,deleted"))
        );
        assert_eq!(
            engine("ReplicatedReplacingMergeTree('/p/{shard}/t', '{replica}', v, d) ORDER BY k"),
            replacing(Some("v"), Some("d"))
        );
        assert_eq!(
            engine("ReplicatedReplacingMergeTree(v) ORDER BY k"),
            replacing(Some("v"), None)
        );
    }

    #[test]
    fn everything_else_is_other() {
        for full in [
            "CollapsingMergeTree(sign) ORDER BY k",
            "SharedMergeTree ORDER BY k",
            "SharedReplacingMergeTree(v) ORDER BY k",
            "Distributed('cluster', 'db', 't', rand())",
            "View",
            "Memory",
            "MaterializedView",
            // arguments it can't read
            "ReplacingMergeTree(v, d, x) ORDER BY k",
            "ReplacingMergeTree(toUInt64(v)) ORDER BY k",
            "ReplacingMergeTree('v') ORDER BY k",
            "MergeTree(d, k, 8192)",
            "ReplacingMergeTree(v ORDER BY k",
        ] {
            assert!(
                matches!(engine(full), TableEngine::Other(_)),
                "{full}: {:?}",
                engine(full)
            );
        }
        assert_eq!(
            engine("CollapsingMergeTree(sign) ORDER BY k").to_string(),
            "CollapsingMergeTree"
        );
    }

    #[test]
    fn clone_engines() {
        assert_eq!(
            TableEngine::MergeTree.clone_engine().as_deref(),
            Some("MergeTree")
        );
        assert_eq!(
            replacing(None, None).clone_engine().as_deref(),
            Some("ReplacingMergeTree()")
        );
        assert_eq!(
            replacing(Some("version"), Some("is deleted"))
                .clone_engine()
                .as_deref(),
            Some("ReplacingMergeTree(version, `is deleted`)")
        );
        assert_eq!(TableEngine::Other("View".into()).clone_engine(), None);
        assert!(TableEngine::is_replicated("ReplicatedReplacingMergeTree"));
        assert!(!TableEngine::is_replicated("ReplacingMergeTree"));
    }
}
