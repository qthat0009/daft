mod arithmetic;
mod comparison;

use daft_core::Series;
#[derive(Clone)]
pub struct ColumnStatistics {
    lower: Series,
    upper: Series,
    count: usize,
    null_count: usize,
    num_bytes: usize,
}

#[derive(PartialEq, Debug)]
pub enum TruthValue {
    False,
    Maybe,
    True,
}

impl std::fmt::Display for TruthValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::False => "False",
            Self::Maybe => "Maybe",
            Self::True => "True",
        };

        write!(f, "TruthValue: {value}",)
    }
}

impl ColumnStatistics {
    pub fn to_truth_value(&self) -> TruthValue {
        let lower = self.lower.bool().unwrap().get(0).unwrap();
        let upper = self.upper.bool().unwrap().get(0).unwrap();
        match (lower, upper) {
            (false, false) => TruthValue::False,
            (false, true) => TruthValue::Maybe,
            (true, true) => TruthValue::True,
            (true, false) => panic!("Upper is false and lower is true; Invalid states!"),
        }
    }

    pub fn from_series(series: &Series) -> Self {
        let lower = series.min(None).unwrap();
        let upper = series.max(None).unwrap();
        let count = series
            .count(None, daft_core::CountMode::All)
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap() as usize;
        let null_count = series
            .count(None, daft_core::CountMode::Null)
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap() as usize;
        let num_bytes = series.size_bytes().unwrap();
        ColumnStatistics {
            lower,
            upper,
            count,
            null_count,
            num_bytes,
        }
    }
}

impl std::fmt::Display for ColumnStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ColumnStatistics:
lower:\n{}
upper:\n{}
count: {}
null_count: {}
num_bytes: {}
",
            self.lower, self.upper, self.count, self.null_count, self.num_bytes
        )
    }
}

impl From<&daft_dsl::LiteralValue> for ColumnStatistics {
    fn from(value: &daft_dsl::LiteralValue) -> Self {
        let ser = value.to_series();
        assert_eq!(ser.len(), 1);
        ColumnStatistics {
            lower: ser.clone(),
            upper: ser.clone(),
            count: ser.len(),
            null_count: 0,
            num_bytes: ser.size_bytes().unwrap(),
        }
    }
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;
    use daft_core::{
        array::ops::DaftCompare,
        datatypes::{Int32Array, Int64Array},
        IntoSeries,
    };

    use super::ColumnStatistics;

    #[test]
    fn test_equal() -> DaftResult<()> {
        let l = ColumnStatistics {
            lower: Int64Array::from(("l", vec![1])).into_series(),
            upper: Int64Array::from(("l", vec![5])).into_series(),
            count: 1,
            null_count: 0,
            num_bytes: 8,
        };
        let r = ColumnStatistics {
            lower: Int32Array::from(("r", vec![4])).into_series(),
            upper: Int32Array::from(("r", vec![6])).into_series(),
            count: 1,
            null_count: 0,
            num_bytes: 8,
        };
        println!("{l}");
        println!("{r}");
        println!("{}", l.lt(&r).to_truth_value());

        Ok(())
    }
}
