//! Postgres' logical replication messages (`pgoutput`, protocol version 1, values as text):
//! the ones a permission provider follows. Begin and Commit come decoded by the replication
//! client; Origin and Type messages are skipped.

use bytes::{Buf, Bytes};

/// A table's columns, as the stream describes it (before its first change, and again when it
/// changes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub name: String,
    /// Part of the replica identity (the key deletes and key changes carry).
    pub key: bool,
}

/// A column's value in a change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    Null,
    /// A TOASTed value the update didn't change (not sent).
    Unchanged,
    Text(String),
}

pub type Tuple = Vec<Cell>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Relation(Relation),
    Insert {
        relation: u32,
        new: Tuple,
    },
    /// `old`: the row before (its key, or all of it with REPLICA IDENTITY FULL), when the key
    /// changed or the identity is full.
    Update {
        relation: u32,
        old: Option<Tuple>,
        new: Tuple,
    },
    Delete {
        relation: u32,
        old: Tuple,
    },
    Truncate {
        relations: Vec<u32>,
    },
    /// Anything else (Origin, Type, …).
    Other(u8),
}

pub fn decode(mut b: Bytes) -> Result<Message, String> {
    let tag = take_u8(&mut b)?;
    Ok(match tag {
        b'R' => {
            let id = take_u32(&mut b)?;
            let namespace = take_str(&mut b)?;
            let name = take_str(&mut b)?;
            let _identity = take_u8(&mut b)?;
            let n = take_u16(&mut b)?;
            let mut columns = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let flags = take_u8(&mut b)?;
                let name = take_str(&mut b)?;
                let _type = take_u32(&mut b)?;
                let _modifier = take_u32(&mut b)?;
                columns.push(RelationColumn {
                    name,
                    key: flags & 1 == 1,
                });
            }
            Message::Relation(Relation {
                id,
                namespace,
                name,
                columns,
            })
        }
        b'I' => {
            let relation = take_u32(&mut b)?;
            expect(&mut b, b'N')?;
            Message::Insert {
                relation,
                new: tuple(&mut b)?,
            }
        }
        b'U' => {
            let relation = take_u32(&mut b)?;
            let mut old = None;
            let mut kind = take_u8(&mut b)?;
            if kind == b'K' || kind == b'O' {
                old = Some(tuple(&mut b)?);
                kind = take_u8(&mut b)?;
            }
            if kind != b'N' {
                return Err(format!("update: expected N, got {:?}", kind as char));
            }
            Message::Update {
                relation,
                old,
                new: tuple(&mut b)?,
            }
        }
        b'D' => {
            let relation = take_u32(&mut b)?;
            let kind = take_u8(&mut b)?;
            if kind != b'K' && kind != b'O' {
                return Err(format!("delete: expected K or O, got {:?}", kind as char));
            }
            Message::Delete {
                relation,
                old: tuple(&mut b)?,
            }
        }
        b'T' => {
            let n = take_u32(&mut b)?;
            let _options = take_u8(&mut b)?;
            let relations = (0..n).map(|_| take_u32(&mut b)).collect::<Result<_, _>>()?;
            Message::Truncate { relations }
        }
        other => Message::Other(other),
    })
}

fn tuple(b: &mut Bytes) -> Result<Tuple, String> {
    let n = take_u16(b)?;
    (0..n)
        .map(|_| match take_u8(b)? {
            b'n' => Ok(Cell::Null),
            b'u' => Ok(Cell::Unchanged),
            b't' => {
                let len = take_u32(b)? as usize;
                if b.remaining() < len {
                    return Err("a value past the message's end".into());
                }
                let bytes = b.split_to(len);
                String::from_utf8(bytes.to_vec())
                    .map(Cell::Text)
                    .map_err(|_| "a text value that isn't UTF-8".into())
            }
            other => Err(format!("unknown value kind {:?}", other as char)),
        })
        .collect()
}

fn expect(b: &mut Bytes, want: u8) -> Result<(), String> {
    match take_u8(b)? {
        got if got == want => Ok(()),
        got => Err(format!(
            "expected {:?}, got {:?}",
            want as char, got as char
        )),
    }
}

fn take_u8(b: &mut Bytes) -> Result<u8, String> {
    (b.remaining() >= 1).then(|| b.get_u8()).ok_or_else(short)
}

fn take_u16(b: &mut Bytes) -> Result<u16, String> {
    (b.remaining() >= 2).then(|| b.get_u16()).ok_or_else(short)
}

fn take_u32(b: &mut Bytes) -> Result<u32, String> {
    (b.remaining() >= 4).then(|| b.get_u32()).ok_or_else(short)
}

fn take_str(b: &mut Bytes) -> Result<String, String> {
    let end = b.iter().position(|&c| c == 0).ok_or_else(short)?;
    let s = b.split_to(end);
    b.advance(1);
    String::from_utf8(s.to_vec()).map_err(|_| "a name that isn't UTF-8".into())
}

fn short() -> String {
    "a message shorter than its contents".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn str(b: &mut BytesMut, s: &str) {
        b.put_slice(s.as_bytes());
        b.put_u8(0);
    }

    fn tuple(b: &mut BytesMut, cells: &[Option<&str>]) {
        b.put_u16(cells.len() as u16);
        for c in cells {
            match c {
                None => b.put_u8(b'n'),
                Some(t) => {
                    b.put_u8(b't');
                    b.put_u32(t.len() as u32);
                    b.put_slice(t.as_bytes());
                }
            }
        }
    }

    fn text(s: &str) -> Cell {
        Cell::Text(s.into())
    }

    #[test]
    fn relations_and_changes_decode() {
        let mut b = BytesMut::new();
        b.put_u8(b'R');
        b.put_u32(16385);
        str(&mut b, "public");
        str(&mut b, "permissions");
        b.put_u8(b'd');
        b.put_u16(2);
        for (flags, name) in [(1, "viewer_id"), (0, "author_id")] {
            b.put_u8(flags);
            str(&mut b, name);
            b.put_u32(25);
            b.put_u32(u32::MAX);
        }
        assert_eq!(
            decode(b.freeze()).unwrap(),
            Message::Relation(Relation {
                id: 16385,
                namespace: "public".into(),
                name: "permissions".into(),
                columns: vec![
                    RelationColumn {
                        name: "viewer_id".into(),
                        key: true
                    },
                    RelationColumn {
                        name: "author_id".into(),
                        key: false
                    },
                ],
            })
        );

        let mut b = BytesMut::new();
        b.put_u8(b'I');
        b.put_u32(16385);
        b.put_u8(b'N');
        tuple(&mut b, &[Some("alice"), Some("bob")]);
        assert_eq!(
            decode(b.freeze()).unwrap(),
            Message::Insert {
                relation: 16385,
                new: vec![text("alice"), text("bob")]
            }
        );

        // an update changing the key: the old key first
        let mut b = BytesMut::new();
        b.put_u8(b'U');
        b.put_u32(16385);
        b.put_u8(b'K');
        tuple(&mut b, &[Some("alice"), None]);
        b.put_u8(b'N');
        b.put_u16(2);
        b.put_u8(b't');
        b.put_u32(4);
        b.put_slice(b"dave");
        b.put_u8(b'u');
        assert_eq!(
            decode(b.freeze()).unwrap(),
            Message::Update {
                relation: 16385,
                old: Some(vec![text("alice"), Cell::Null]),
                new: vec![text("dave"), Cell::Unchanged],
            }
        );

        let mut b = BytesMut::new();
        b.put_u8(b'D');
        b.put_u32(16385);
        b.put_u8(b'K');
        tuple(&mut b, &[Some("alice"), None]);
        assert_eq!(
            decode(b.freeze()).unwrap(),
            Message::Delete {
                relation: 16385,
                old: vec![text("alice"), Cell::Null]
            }
        );

        let mut b = BytesMut::new();
        b.put_u8(b'T');
        b.put_u32(2);
        b.put_u8(0);
        b.put_u32(1);
        b.put_u32(2);
        assert_eq!(
            decode(b.freeze()).unwrap(),
            Message::Truncate {
                relations: vec![1, 2]
            }
        );
        assert_eq!(
            decode(Bytes::from_static(b"O\0")).unwrap(),
            Message::Other(b'O')
        );
    }

    #[test]
    fn truncated_messages_are_errors() {
        let mut b = BytesMut::new();
        b.put_u8(b'I');
        b.put_u32(1);
        b.put_u8(b'N');
        b.put_u16(1);
        b.put_u8(b't');
        b.put_u32(10);
        b.put_slice(b"abc");
        assert!(decode(b.freeze()).is_err());
        assert!(decode(Bytes::from_static(b"I\0\0")).is_err());
    }
}
