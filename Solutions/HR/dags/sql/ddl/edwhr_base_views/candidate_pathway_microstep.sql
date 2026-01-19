create or replace view `{{ params.param_hr_base_views_dataset_name }}.candidate_pathway_microstep`
AS SELECT
    candidate_pathway_microstep.candidate_profile_sid,
    candidate_pathway_microstep.pathway_id,
    candidate_pathway_microstep.microstep_num,
    candidate_pathway_microstep.microstep_start_date_time,
    candidate_pathway_microstep.microstep_end_date_time,
    candidate_pathway_microstep.source_system_code,
    candidate_pathway_microstep.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_pathway_microstep;