CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_smoking_status AS SELECT
    ref_smoking_status.smoking_status_id,
    ref_smoking_status.smoking_status_desc,
    ref_smoking_status.active_dw_ind,
    ref_smoking_status.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_smoking_status
;
