use crate::{
    datatypes::{
        logical::{DateArray, TimeArray, TimestampArray},
        Field, Int32Array, Int64Array, TimeUnit, UInt32Array,
    },
    DataType,
};
use arrow2::compute::arithmetics::ArraySub;
use chrono::{NaiveDate, Timelike};
use common_error::{DaftError, DaftResult};

use super::as_arrow::AsArrow;

impl DateArray {
    pub fn day(&self) -> DaftResult<UInt32Array> {
        let input_array = self
            .physical
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);
        let day_arr = arrow2::compute::temporal::day(&input_array)?;
        Ok((self.name(), Box::new(day_arr)).into())
    }

    pub fn month(&self) -> DaftResult<UInt32Array> {
        let input_array = self
            .physical
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);
        let month_arr = arrow2::compute::temporal::month(&input_array)?;
        Ok((self.name(), Box::new(month_arr)).into())
    }

    pub fn year(&self) -> DaftResult<Int32Array> {
        let input_array = self
            .physical
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);
        let year_arr = arrow2::compute::temporal::year(&input_array)?;
        Ok((self.name(), Box::new(year_arr)).into())
    }

    pub fn day_of_week(&self) -> DaftResult<UInt32Array> {
        let input_array = self
            .physical
            .as_arrow()
            .clone()
            .to(arrow2::datatypes::DataType::Date32);
        let day_arr = arrow2::compute::temporal::weekday(&input_array)?;
        Ok((self.name(), Box::new(day_arr.sub(&1))).into())
    }
}

impl TimestampArray {
    pub fn date(&self) -> DaftResult<DateArray> {
        let physical = self.physical.as_arrow();
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = timeunit.to_arrow();
        let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date_arrow = match tz {
            Some(tz) => {
                if let Ok(tz) = arrow2::temporal_conversions::parse_offset(tz) {
                    Ok(arrow2::array::PrimitiveArray::<i32>::from_iter(
                        physical.iter().map(|ts| {
                            ts.map(|ts| {
                                (arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz)
                                    .date_naive()
                                    - epoch_date)
                                    .num_days() as i32
                            })
                        }),
                    ))
                } else if let Ok(tz) = arrow2::temporal_conversions::parse_offset_tz(tz) {
                    Ok(arrow2::array::PrimitiveArray::<i32>::from_iter(
                        physical.iter().map(|ts| {
                            ts.map(|ts| {
                                (arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz)
                                    .date_naive()
                                    - epoch_date)
                                    .num_days() as i32
                            })
                        }),
                    ))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Cannot parse timezone in Timestamp datatype: {}",
                        tz
                    )))
                }
            }
            None => Ok(arrow2::array::PrimitiveArray::<i32>::from_iter(
                physical.iter().map(|ts| {
                    ts.map(|ts| {
                        (arrow2::temporal_conversions::timestamp_to_naive_datetime(*ts, tu).date()
                            - epoch_date)
                            .num_days() as i32
                    })
                }),
            )),
        }?;
        Ok(DateArray::new(
            Field::new(self.name(), DataType::Date),
            Int32Array::from((self.name(), Box::new(date_arrow))),
        ))
    }

    pub fn time(&self, timeunit_for_cast: &TimeUnit) -> DaftResult<TimeArray> {
        let physical = self.physical.as_arrow();
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = timeunit.to_arrow();
        let timeunit_for_cast = if timeunit_for_cast == &TimeUnit::Nanoseconds {
            TimeUnit::Nanoseconds
        } else {
            TimeUnit::Microseconds // default to microseconds
        };
        let time_arrow = match tz {
            Some(tz) => match arrow2::temporal_conversions::parse_offset(tz) {
                Ok(tz) => Ok(arrow2::array::PrimitiveArray::<i64>::from_iter(
                    physical.iter().map(|ts| {
                        ts.map(|ts| {
                            let dt =
                                arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz);
                            match timeunit_for_cast {
                                TimeUnit::Nanoseconds => {
                                    let hour = dt.hour() as i64 * 3_600_000_000_000;
                                    let minute = dt.minute() as i64 * 60_000_000_000;
                                    let second = dt.second() as i64 * 1_000_000_000;
                                    let nanosecond = dt.nanosecond() as i64;
                                    hour + minute + second + nanosecond
                                }
                                _ => {
                                    let hour = dt.hour() as i64 * 3_600_000_000;
                                    let minute = dt.minute() as i64 * 60_000_000;
                                    let second = dt.second() as i64 * 1_000_000;
                                    let microsecond = dt.nanosecond() as i64 / 1_000;
                                    hour + minute + second + microsecond
                                }
                            }
                        })
                    }),
                )),
                Err(e) => Err(DaftError::TypeError(format!(
                    "Cannot parse timezone in Timestamp datatype: {}, error: {}",
                    tz, e
                ))),
            },
            None => Ok(arrow2::array::PrimitiveArray::<i64>::from_iter(
                physical.iter().map(|ts| {
                    ts.map(|ts| {
                        let dt = arrow2::temporal_conversions::timestamp_to_naive_datetime(*ts, tu);
                        match timeunit_for_cast {
                            TimeUnit::Nanoseconds => {
                                let hour = dt.hour() as i64 * 3_600_000_000_000;
                                let minute = dt.minute() as i64 * 60_000_000_000;
                                let second = dt.second() as i64 * 1_000_000_000;
                                let nanosecond = dt.nanosecond() as i64;
                                hour + minute + second + nanosecond
                            }
                            _ => {
                                let hour = dt.hour() as i64 * 3_600_000_000;
                                let minute = dt.minute() as i64 * 60_000_000;
                                let second = dt.second() as i64 * 1_000_000;
                                let microsecond = dt.nanosecond() as i64 / 1_000;
                                hour + minute + second + microsecond
                            }
                        }
                    })
                }),
            )),
        }?;
        Ok(TimeArray::new(
            Field::new(self.name(), DataType::Time(timeunit_for_cast)),
            Int64Array::from((self.name(), Box::new(time_arrow))),
        ))
    }

    pub fn hour(&self) -> DaftResult<UInt32Array> {
        let physical = self.physical.as_arrow();
        let DataType::Timestamp(timeunit, tz) = self.data_type() else {
            unreachable!("Timestamp array must have Timestamp datatype")
        };
        let tu = timeunit.to_arrow();
        let date_arrow = match tz {
            Some(tz) => match arrow2::temporal_conversions::parse_offset(tz) {
                Ok(tz) => Ok(arrow2::array::UInt32Array::from_iter(physical.iter().map(
                    |ts| {
                        ts.map(|ts| {
                            arrow2::temporal_conversions::timestamp_to_datetime(*ts, tu, &tz).hour()
                        })
                    },
                ))),
                Err(e) => Err(DaftError::TypeError(format!(
                    "Cannot parse timezone in Timestamp datatype: {}, error: {}",
                    tz, e
                ))),
            },
            None => Ok(arrow2::array::UInt32Array::from_iter(physical.iter().map(
                |ts| {
                    ts.map(|ts| {
                        arrow2::temporal_conversions::timestamp_to_naive_datetime(*ts, tu).hour()
                    })
                },
            ))),
        }?;

        UInt32Array::new(
            std::sync::Arc::new(Field::new(self.name(), DataType::UInt32)),
            Box::new(date_arrow),
        )
    }
}
