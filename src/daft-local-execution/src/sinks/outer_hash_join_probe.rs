use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    prelude::{
        bitmap::{or, Bitmap, MutableBitmap},
        Schema, SchemaRef,
    },
    series::Series,
};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use daft_table::{GrowableTable, ProbeState, Table};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use super::streaming_sink::{StreamingSink, StreamingSinkOutput, StreamingSinkState};
use crate::pipeline::PipelineResultType;

struct IndexBitmapBuilder {
    bitmap: MutableBitmap,
    prefix_sums: Vec<usize>,
}

impl IndexBitmapBuilder {
    fn new(tables: &[Table]) -> Self {
        let prefix_sums = tables
            .iter()
            .map(|t| t.len())
            .scan(0, |acc, x| {
                let prev = *acc;
                *acc += x;
                Some(prev)
            })
            .collect::<Vec<_>>();
        let total_len = prefix_sums.last().unwrap() + tables.last().unwrap().len();
        Self {
            bitmap: MutableBitmap::from_len_zeroed(total_len),
            prefix_sums,
        }
    }

    #[inline]
    fn mark_used(&mut self, table_idx: usize, row_idx: usize) {
        let idx = self.prefix_sums[table_idx] + row_idx;
        self.bitmap.set(idx, true);
    }

    fn build(self) -> IndexBitmap {
        IndexBitmap {
            bitmap: self.bitmap.into(),
            prefix_sums: self.prefix_sums,
        }
    }
}

struct IndexBitmap {
    bitmap: Bitmap,
    prefix_sums: Vec<usize>,
}

impl IndexBitmap {
    fn or(&self, other: &Self) -> Self {
        assert_eq!(self.prefix_sums, other.prefix_sums);
        Self {
            bitmap: or(&self.bitmap, &other.bitmap),
            prefix_sums: self.prefix_sums.clone(),
        }
    }

    fn unset_bits(&self) -> usize {
        self.bitmap.unset_bits()
    }

    fn get_unused_indices(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        let mut curr_table = 0;
        self.bitmap
            .iter()
            .enumerate()
            .filter_map(move |(idx, is_set)| {
                if is_set {
                    None
                } else {
                    while curr_table < self.prefix_sums.len() - 1
                        && idx >= self.prefix_sums[curr_table + 1]
                    {
                        curr_table += 1;
                    }
                    let row_idx = idx - self.prefix_sums[curr_table];
                    Some((curr_table, row_idx))
                }
            })
    }
}

enum OuterHashJoinProbeState {
    Building,
    ReadyToProbe(Arc<ProbeState>, Option<IndexBitmapBuilder>),
}

impl OuterHashJoinProbeState {
    fn initialize_probe_state(&mut self, probe_state: Arc<ProbeState>, needs_bitmap: bool) {
        let tables = probe_state.get_tables().clone();
        if let Self::Building = self {
            *self = Self::ReadyToProbe(
                probe_state,
                if needs_bitmap {
                    Some(IndexBitmapBuilder::new(&tables))
                } else {
                    None
                },
            );
        } else {
            panic!("OuterHashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn get_probe_state(&self) -> &ProbeState {
        if let Self::ReadyToProbe(probe_state, _) = self {
            probe_state
        } else {
            panic!("get_probeable_and_table can only be used during the ReadyToProbe Phase")
        }
    }

    fn get_bitmap_builder(&mut self) -> &mut Option<IndexBitmapBuilder> {
        if let Self::ReadyToProbe(_, bitmap_builder) = self {
            bitmap_builder
        } else {
            panic!("get_bitmap can only be used during the ReadyToProbe Phase")
        }
    }
}

impl StreamingSinkState for OuterHashJoinProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(crate) struct OuterHashJoinProbeSink {
    probe_on: Vec<ExprRef>,
    common_join_keys: Vec<String>,
    left_non_join_columns: Vec<String>,
    right_non_join_columns: Vec<String>,
    right_non_join_schema: SchemaRef,
    join_type: JoinType,
}

impl OuterHashJoinProbeSink {
    pub(crate) fn new(
        probe_on: Vec<ExprRef>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        join_type: JoinType,
        common_join_keys: IndexSet<String>,
    ) -> Self {
        let left_non_join_columns = left_schema
            .fields
            .keys()
            .filter(|c| !common_join_keys.contains(*c))
            .cloned()
            .collect();
        let right_non_join_fields = right_schema
            .fields
            .values()
            .filter(|f| !common_join_keys.contains(&f.name))
            .cloned()
            .collect();
        let right_non_join_schema =
            Arc::new(Schema::new(right_non_join_fields).expect("right schema should be valid"));
        let right_non_join_columns = right_non_join_schema.fields.keys().cloned().collect();
        let common_join_keys = common_join_keys.into_iter().collect();
        Self {
            probe_on,
            common_join_keys,
            left_non_join_columns,
            right_non_join_columns,
            right_non_join_schema,
            join_type,
        }
    }

    fn probe_left_right(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut OuterHashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = {
            let probe_state = state.get_probe_state();
            (probe_state.get_probeable(), probe_state.get_tables())
        };

        let _growables = info_span!("OuterHashJoinProbeSink::build_growables").entered();

        let mut build_side_growable = GrowableTable::new(
            &tables.iter().collect::<Vec<_>>(),
            true,
            tables.iter().map(|t| t.len()).sum(),
        )?;

        let input_tables = input.get_tables()?;

        let mut probe_side_growable =
            GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, input.len())?;

        drop(_growables);
        {
            let _loop = info_span!("OuterHashJoinProbeSink::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(&self.probe_on)?;
                let idx_mapper = probe_table.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_mapper.make_iter().enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_side_table_idx, build_row_idx) in inner_iter {
                            build_side_growable.extend(
                                build_side_table_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                        }
                    } else {
                        // if there's no match, we should still emit the probe side and fill the build side with nulls
                        build_side_growable.add_nulls(1);
                        probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                    }
                }
            }
        }
        let build_side_table = build_side_growable.build()?;
        let probe_side_table = probe_side_growable.build()?;

        let final_table = if self.join_type == JoinType::Left {
            let join_table = probe_side_table.get_columns(&self.common_join_keys)?;
            let left = probe_side_table.get_columns(&self.left_non_join_columns)?;
            let right = build_side_table.get_columns(&self.right_non_join_columns)?;
            join_table.union(&left)?.union(&right)?
        } else {
            let join_table = probe_side_table.get_columns(&self.common_join_keys)?;
            let left = build_side_table.get_columns(&self.left_non_join_columns)?;
            let right = probe_side_table.get_columns(&self.right_non_join_columns)?;
            join_table.union(&left)?.union(&right)?
        };
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }

    fn probe_outer(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut OuterHashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = {
            let probe_state = state.get_probe_state();
            (probe_state.get_probeable(), probe_state.get_tables())
        };
        let bitmap_builder = state.get_bitmap_builder();
        let _growables = info_span!("OuterHashJoinProbeSink::build_growables").entered();

        // Need to set use_validity to true here because we add nulls to the build side
        let mut build_side_growable = GrowableTable::new(
            &tables.iter().collect::<Vec<_>>(),
            true,
            tables.iter().map(|t| t.len()).sum(),
        )?;

        let input_tables = input.get_tables()?;

        let mut probe_side_growable =
            GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, input.len())?;

        let left_idx_used = bitmap_builder
            .as_mut()
            .expect("bitmap should be set in outer join");

        drop(_growables);
        {
            let _loop = info_span!("OuterHashJoinProbeSink::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(&self.probe_on)?;
                let idx_mapper = probe_table.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_mapper.make_iter().enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_side_table_idx, build_row_idx) in inner_iter {
                            let build_side_table_idx = build_side_table_idx as usize;
                            let build_row_idx = build_row_idx as usize;
                            left_idx_used.mark_used(build_side_table_idx, build_row_idx);
                            build_side_growable.extend(build_side_table_idx, build_row_idx, 1);
                            probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                        }
                    } else {
                        // if there's no match, we should still emit the probe side and fill the build side with nulls
                        build_side_growable.add_nulls(1);
                        probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                    }
                }
            }
        }
        let build_side_table = build_side_growable.build()?;
        let probe_side_table = probe_side_growable.build()?;

        let join_table = probe_side_table.get_columns(&self.common_join_keys)?;
        let left = build_side_table.get_columns(&self.left_non_join_columns)?;
        let right = probe_side_table.get_columns(&self.right_non_join_columns)?;
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }

    fn finalize_outer(
        &self,
        mut states: Vec<Box<dyn StreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let states = states
            .iter_mut()
            .map(|s| {
                s.as_any_mut()
                    .downcast_mut::<OuterHashJoinProbeState>()
                    .expect("OuterHashJoinProbeSink state should be OuterHashJoinProbeState")
            })
            .collect::<Vec<_>>();
        let tables = states
            .first()
            .expect("at least one state should be present")
            .get_probe_state()
            .get_tables();

        let merged_bitmap = {
            let bitmaps = states.into_iter().map(|s| {
                if let OuterHashJoinProbeState::ReadyToProbe(_, bitmap) = s {
                    bitmap
                        .take()
                        .expect("bitmap should be present in outer join")
                        .build()
                } else {
                    panic!("OuterHashJoinProbeState should be in ReadyToProbe state")
                }
            });
            bitmaps.fold(None, |acc, x| match acc {
                None => Some(x),
                Some(acc) => Some(acc.or(&x)),
            })
        }
        .expect("at least one bitmap should be present");

        let mut build_side_growable = GrowableTable::new(
            &tables.iter().collect::<Vec<_>>(),
            true,
            merged_bitmap.unset_bits(),
        )?;

        for (table_idx, row_idx) in merged_bitmap.get_unused_indices() {
            build_side_growable.extend(table_idx, row_idx, 1);
        }

        let build_side_table = build_side_growable.build()?;

        let join_table = build_side_table.get_columns(&self.common_join_keys)?;
        let left = build_side_table.get_columns(&self.left_non_join_columns)?;
        let right = {
            let columns = self
                .right_non_join_schema
                .fields
                .values()
                .map(|field| Series::full_null(&field.name, &field.dtype, left.len()))
                .collect::<Vec<_>>();
            Table::new_unchecked(self.right_non_join_schema.clone(), columns, left.len())
        };
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Some(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ))))
    }
}

impl StreamingSink for OuterHashJoinProbeSink {
    #[instrument(skip_all, name = "OuterHashJoinProbeSink::execute")]
    fn execute(
        &self,
        idx: usize,
        input: &PipelineResultType,
        state: &mut dyn StreamingSinkState,
    ) -> DaftResult<StreamingSinkOutput> {
        match idx {
            0 => {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<OuterHashJoinProbeState>()
                    .expect("OuterHashJoinProbeSink state should be OuterHashJoinProbeState");
                let probe_state = input.as_probe_state();
                state
                    .initialize_probe_state(probe_state.clone(), self.join_type == JoinType::Outer);
                Ok(StreamingSinkOutput::NeedMoreInput(None))
            }
            _ => {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<OuterHashJoinProbeState>()
                    .expect("OuterHashJoinProbeSink state should be OuterHashJoinProbeState");
                let input = input.as_data();
                let out = match self.join_type {
                    JoinType::Left | JoinType::Right => self.probe_left_right(input, state),
                    JoinType::Outer => self.probe_outer(input, state),
                    _ => unreachable!(
                        "Only Left, Right, and Outer joins are supported in OuterHashJoinProbeSink"
                    ),
                }?;
                Ok(StreamingSinkOutput::NeedMoreInput(Some(out)))
            }
        }
    }

    fn name(&self) -> &'static str {
        "OuterHashJoinProbeSink"
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(OuterHashJoinProbeState::Building)
    }

    fn finalize(
        &self,
        states: Vec<Box<dyn StreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.join_type == JoinType::Outer {
            self.finalize_outer(states)
        } else {
            Ok(None)
        }
    }
}
