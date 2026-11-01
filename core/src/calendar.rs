//! `now() ± INTERVAL …` as ClickHouse computes it: fixed units (MILLISECOND to HOUR) add a
//! duration; DAY and WEEK go by the calendar in the timezone (`now('Europe/Warsaw')`, or the
//! server's), so across a DST change a day is 23 or 25 hours. A local time that a DST change
//! skips takes the offset after it; one it repeats, the earlier instant — as ClickHouse does.

use chrono::{DateTime, Days, LocalResult, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;

const NS: i128 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl Unit {
    pub fn name(self) -> &'static str {
        match self {
            Unit::Millisecond => "MILLISECOND",
            Unit::Second => "SECOND",
            Unit::Minute => "MINUTE",
            Unit::Hour => "HOUR",
            Unit::Day => "DAY",
            Unit::Week => "WEEK",
        }
    }

    /// Its length, for the fixed units; DAY and WEEK by the calendar.
    fn fixed_ns(self) -> Option<i128> {
        match self {
            Unit::Millisecond => Some(NS / 1000),
            Unit::Second => Some(NS),
            Unit::Minute => Some(60 * NS),
            Unit::Hour => Some(3600 * NS),
            Unit::Day | Unit::Week => None,
        }
    }

    /// Days, for the calendar units.
    fn days(self) -> i64 {
        match self {
            Unit::Week => 7,
            _ => 1,
        }
    }
}

/// `± INTERVAL n unit`; `amount` carries the sign.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub amount: i64,
    pub unit: Unit,
}

/// Whether the intervals need the calendar (DAY or WEEK, in a zone with offset changes).
fn by_calendar(intervals: &[Interval], tz: Tz) -> bool {
    tz != Tz::UTC
        && intervals
            .iter()
            .any(|i| matches!(i.unit, Unit::Day | Unit::Week))
}

/// The intervals as nanoseconds, a day as 24 hours.
fn naive_ns(intervals: &[Interval]) -> i128 {
    intervals
        .iter()
        .map(|i| {
            i128::from(i.amount)
                * i.unit
                    .fixed_ns()
                    .unwrap_or(86_400 * NS * i128::from(i.unit.days()))
        })
        .sum()
}

/// A local date-time in `tz` as an instant, as ClickHouse resolves it.
fn instant(local: NaiveDateTime, tz: Tz) -> DateTime<Utc> {
    match tz.from_local_datetime(&local) {
        LocalResult::Single(t) => t.with_timezone(&Utc),
        // repeated (the clocks went back): the earlier
        LocalResult::Ambiguous(earlier, _) => earlier.with_timezone(&Utc),
        // skipped (the clocks went forward): with the offset after the gap
        LocalResult::None => {
            let mut later = local;
            for _ in 0..(24 * 60) {
                later += chrono::Duration::minutes(1);
                if let Some(t) = tz.from_local_datetime(&later).earliest() {
                    return (t - (later - local)).with_timezone(&Utc);
                }
            }
            Utc.from_utc_datetime(&local)
        }
    }
}

/// `now` (nanoseconds since the epoch) with the intervals applied left to right.
pub fn shift(now: i128, intervals: &[Interval], tz: Tz) -> i128 {
    if !by_calendar(intervals, tz) {
        return now + naive_ns(intervals);
    }
    let mut t = now;
    for i in intervals {
        t = match i.unit.fixed_ns() {
            Some(per) => t + i128::from(i.amount) * per,
            None => {
                let (secs, nanos) = (t.div_euclid(NS) as i64, t.rem_euclid(NS) as u32);
                let utc = DateTime::from_timestamp(secs, nanos).expect("an instant in range");
                let local = utc.with_timezone(&tz).naive_local();
                let days = Days::new((i.amount.unsigned_abs()) * i.unit.days() as u64);
                let moved = if i.amount >= 0 {
                    local.checked_add_days(days)
                } else {
                    local.checked_sub_days(days)
                }
                .expect("a date in range");
                let back = instant(moved, tz);
                i128::from(back.timestamp()) * NS + i128::from(back.timestamp_subsec_nanos())
            }
        };
    }
    t
}

/// When a row with `ts` leaves `col > now() + intervals` (`>=` if `inclusive`): the first
/// instant at which the condition no longer holds. Around a skipped local time ClickHouse's own
/// value goes back for a while; the row leaves at the first instant and doesn't come back.
pub fn deadline(ts: i128, intervals: &[Interval], tz: Tz, inclusive: bool) -> i128 {
    let gone = |n: i128| {
        let s = shift(n, intervals, tz);
        if inclusive {
            s > ts
        } else {
            s >= ts
        }
    };
    let guess = ts - naive_ns(intervals) + i128::from(inclusive);
    if !by_calendar(intervals, tz) {
        return guess;
    }
    // the calendar moves it by the DST changes in between: a few hours at most
    let (mut lo, mut hi) = (guess - 4 * 3600 * NS, guess + 4 * 3600 * NS);
    while gone(lo) {
        lo -= 24 * 3600 * NS;
    }
    while !gone(hi) {
        hi += 24 * 3600 * NS;
    }
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if gone(mid) {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    hi
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i128 = 3600 * NS;
    const WARSAW: Tz = chrono_tz::Europe::Warsaw;

    fn at(local: &str, tz: Tz) -> i128 {
        let n = NaiveDateTime::parse_from_str(local, "%Y-%m-%d %H:%M:%S").unwrap();
        let t = instant(n, tz);
        i128::from(t.timestamp()) * NS
    }

    fn day(amount: i64) -> Vec<Interval> {
        vec![Interval {
            amount,
            unit: Unit::Day,
        }]
    }

    #[test]
    fn days_go_by_the_calendar_across_dst() {
        // the clocks go back on 2026-10-25, forward on 2026-03-29
        let fall = at("2026-10-24 12:00:00", WARSAW);
        assert_eq!(shift(fall, &day(1), WARSAW) - fall, 25 * H);
        let spring = at("2026-03-28 12:00:00", WARSAW);
        assert_eq!(shift(spring, &day(1), WARSAW) - spring, 23 * H);
        assert_eq!(shift(spring, &day(1), Tz::UTC) - spring, 24 * H);
        // into the skipped hour: the offset after it (23 h, as ClickHouse)
        let gap = at("2026-03-28 02:30:00", WARSAW);
        assert_eq!(shift(gap, &day(1), WARSAW) - gap, 23 * H);
        // into the repeated hour: the earlier one (24 h from the day before; 25 h back from
        // the day after)
        let before = at("2026-10-24 02:30:00", WARSAW);
        assert_eq!(shift(before, &day(1), WARSAW) - before, 24 * H);
        let after = at("2026-10-26 02:30:00", WARSAW);
        assert_eq!(shift(after, &day(-1), WARSAW) - after, -25 * H);
        // left to right, with fixed units
        let both = vec![
            Interval {
                amount: -1,
                unit: Unit::Week,
            },
            Interval {
                amount: 2,
                unit: Unit::Hour,
            },
        ];
        let t = at("2026-10-30 12:00:00", WARSAW);
        assert_eq!(shift(t, &both, WARSAW) - t, -(7 * 24 + 1) * H + 2 * H);
    }

    #[test]
    fn a_deadline_is_where_the_condition_stops_holding() {
        for (ts, intervals) in [
            (at("2026-10-25 12:00:00", WARSAW), day(-1)),
            (at("2026-03-29 12:00:00", WARSAW), day(-1)),
            (at("2026-10-26 00:00:00", WARSAW), day(-2)),
            (at("2026-06-01 00:00:00", WARSAW), day(-1)),
        ] {
            for inclusive in [false, true] {
                let d = deadline(ts, &intervals, WARSAW, inclusive);
                let holds = |n: i128| {
                    let s = shift(n, &intervals, WARSAW);
                    if inclusive {
                        ts >= s
                    } else {
                        ts > s
                    }
                };
                assert!(!holds(d), "{ts} {inclusive}: gone at the deadline");
                assert!(holds(d - 1), "{ts} {inclusive}: there just before");
            }
        }
        // a day after the fall-back's previous day: 25 h
        let ts = at("2026-10-24 12:00:00", WARSAW);
        assert_eq!(deadline(ts, &day(-1), WARSAW, false) - ts, 25 * H);
        // the fast path: plain subtraction
        let fixed = vec![Interval {
            amount: -10,
            unit: Unit::Second,
        }];
        assert_eq!(deadline(ts, &fixed, WARSAW, false), ts + 10 * NS);
        assert_eq!(deadline(ts, &fixed, WARSAW, true), ts + 10 * NS + 1);
        assert_eq!(deadline(ts, &day(-1), Tz::UTC, false), ts + 24 * H);
    }
}
