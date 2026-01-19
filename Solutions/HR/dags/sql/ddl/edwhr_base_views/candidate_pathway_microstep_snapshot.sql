CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.candidate_pathway_microstep_snapshot AS SELECT
    candidate_pathway_microstep_snapshot.candidate_profile_sid,
    candidate_pathway_microstep_snapshot.pathway_id,
    candidate_pathway_microstep_snapshot.microstep_num,
    candidate_pathway_microstep_snapshot.snapshot_date,
    candidate_pathway_microstep_snapshot.microstep_start_date_time,
    candidate_pathway_microstep_snapshot.microstep_end_date_time,
    candidate_pathway_microstep_snapshot.source_system_code,
    candidate_pathway_microstep_snapshot.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_pathway_microstep_snapshot