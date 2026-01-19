/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_pathway_microstep_snapshot AS SELECT
      a.candidate_profile_sid,
      a.pathway_id,
      a.microstep_num,
      a.snapshot_date,
      a.microstep_start_date_time,
      a.microstep_end_date_time,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_pathway_microstep_snapshot AS a
  ;

