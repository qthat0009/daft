use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum StatsState {
    NotMaterialized,
    Materialized(PlanStats),
}

impl Default for StatsState {
    fn default() -> Self {
        Self::NotMaterialized
    }
}

impl StatsState {
    pub fn get_stats(&self) -> &PlanStats {
        match self {
            Self::Materialized(stats) => stats,
            Self::NotMaterialized => {
                panic!("Tried getting stats from a StatsState that is not materialized")
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanStats {
    // Currently we're only putting cardinality stats in the plan stats.
    // In the future we want to start including column stats, including min, max, NDVs, etc.
    pub approx_stats: ApproxStats,
}

impl PlanStats {
    pub fn new(approx_stats: ApproxStats) -> Self {
        Self { approx_stats }
    }

    pub fn empty() -> Self {
        Self {
            approx_stats: ApproxStats::empty(),
        }
    }
}

// We implement PartialEq, Eq, and Hash so that all PlanStats are considered equal. This allows
// logical/physical plans that are enriched with stats to easily implement PartialEq, Eq, and Hash
// in a way that ignores PlanStats when considering equality.

impl PartialEq for PlanStats {
    #[inline]
    fn eq(&self, _other: &Self) -> bool {
        true // All PlanStats are considered equal.
    }
}

impl Eq for PlanStats {}

use std::hash::Hash;
impl Hash for PlanStats {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        // Add nothing to hash state since all PlanStats should hash the same.
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ApproxStats {
    pub lower_bound_rows: usize,
    pub upper_bound_rows: Option<usize>,
    pub lower_bound_bytes: usize,
    pub upper_bound_bytes: Option<usize>,
}

impl ApproxStats {
    pub fn empty() -> Self {
        Self {
            lower_bound_rows: 0,
            upper_bound_rows: None,
            lower_bound_bytes: 0,
            upper_bound_bytes: None,
        }
    }
    pub fn apply<F: Fn(usize) -> usize>(&self, f: F) -> Self {
        Self {
            lower_bound_rows: f(self.lower_bound_rows),
            upper_bound_rows: self.upper_bound_rows.map(&f),
            lower_bound_bytes: f(self.lower_bound_rows),
            upper_bound_bytes: self.upper_bound_bytes.map(&f),
        }
    }
}

use std::ops::Add;
impl Add for &ApproxStats {
    type Output = ApproxStats;
    fn add(self, rhs: Self) -> Self::Output {
        ApproxStats {
            lower_bound_rows: self.lower_bound_rows + rhs.lower_bound_rows,
            upper_bound_rows: self
                .upper_bound_rows
                .and_then(|l_ub| rhs.upper_bound_rows.map(|v| v + l_ub)),
            lower_bound_bytes: self.lower_bound_bytes + rhs.lower_bound_bytes,
            upper_bound_bytes: self
                .upper_bound_bytes
                .and_then(|l_ub| rhs.upper_bound_bytes.map(|v| v + l_ub)),
        }
    }
}
