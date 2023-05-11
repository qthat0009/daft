use crate::{
    array::DataArray,
    datatypes::UInt64Array,
    error::{DaftError, DaftResult},
    kernels::search_sorted::search_sorted_multi_array,
    series::Series,
    table::Table,
};

impl Table {
    pub fn search_sorted(&self, keys: &Self, descending: &[bool]) -> DaftResult<UInt64Array> {
        if self.schema != keys.schema {
            return Err(DaftError::SchemaMismatch(format!(
                "Schema Mismatch in search_sorted: data: {} vs keys: {}",
                self.schema, keys.schema
            )));
        }
        if self.num_columns() != descending.len() {
            return Err(DaftError::ValueError(format!("Mismatch in number of arguments for `descending` in search sorted: num_columns: {} vs : descending.len() {}", self.num_columns(), descending.len())));
        }

        if self.num_columns() == 1 {
            return self
                .get_column_by_index(0)?
                .search_sorted(keys.get_column_by_index(0)?, *descending.first().unwrap());
        }
        unsafe {
            multicol_search_sorted(self.columns.as_slice(), keys.columns.as_slice(), descending)
        }
    }
}

unsafe fn multicol_search_sorted(
    data: &[Series],
    keys: &[Series],
    descending: &[bool],
) -> DaftResult<UInt64Array> {
    let data_arrow_vec = data.iter().map(|s| s.array()).collect();
    let keys_arrow_vec = keys.iter().map(|s| s.array()).collect();

    let indices =
        search_sorted_multi_array(&data_arrow_vec, &keys_arrow_vec, &Vec::from(descending))?;
    Ok(DataArray::from(("indices", Box::new(indices))))
}
