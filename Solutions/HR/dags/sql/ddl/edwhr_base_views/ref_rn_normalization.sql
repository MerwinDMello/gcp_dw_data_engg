CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_rn_normalization AS SELECT
    ref_rn_normalization.job_title_text,
    ref_rn_normalization.skill_mix_desc,
    ref_rn_normalization.auxiliary_status_code,
    ref_rn_normalization.normalized_skill_mix_desc,
    ref_rn_normalization.normalized_auxiliary_status_code,
    ref_rn_normalization.source_system_code,
    ref_rn_normalization.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_rn_normalization
;