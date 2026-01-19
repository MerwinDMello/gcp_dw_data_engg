/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_coid_hospital_category AS SELECT
      a.coid,
      a.company_code,
      a.year_num,
      a.hospital_level_code,
      a.prev_year_change_ind,
      a.manual_hold_ind,
      a.quartile_rank_num,
      a.total_rank_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_coid_hospital_category AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.secref_facility AS b ON a.company_code = b.company_code
       AND a.coid = b.co_id
       AND b.user_id = session_user()
  ;

