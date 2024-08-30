use common_error::DaftResult;

use crate::{
    array::ops::{as_arrow::AsArrow, DaftApproxCountDistinctMergeAggable},
    datatypes::{FixedSizeBinaryArray, UInt64Array},
};
use hyperloglog::HyperLogLog;

use crate::array::ops::GroupIndices;

impl DaftApproxCountDistinctMergeAggable for FixedSizeBinaryArray {
    type Output = DaftResult<UInt64Array>;

    fn approx_count_distinct_merge(&self) -> Self::Output {
        let mut final_hll = HyperLogLog::default();
        for byte_slice in self.as_arrow().values_iter() {
            let hll = HyperLogLog::new_with_byte_slice(byte_slice);
            final_hll.merge(&hll);
        }
        let count = final_hll.count() as u64;
        let data = &[count] as &[_];
        let array = (self.name(), data).into();
        Ok(array)
    }

    fn grouped_approx_count_distinct_merge(&self, groups: &GroupIndices) -> Self::Output {
        let data = self.as_arrow();
        let mut counts = Vec::with_capacity(groups.len());
        for group in groups {
            let mut final_hll = HyperLogLog::default();
            for &index in group {
                if let Some(byte_slice) = data.get(index as _) {
                    let hll = HyperLogLog::new_with_byte_slice(byte_slice);
                    final_hll.merge(&hll);
                }
            }
            let count = final_hll.count() as u64;
            counts.push(count);
        }
        let array = (self.name(), counts.as_slice()).into();
        Ok(array)
    }
}
