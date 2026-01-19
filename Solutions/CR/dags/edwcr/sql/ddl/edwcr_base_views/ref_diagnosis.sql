CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_diagnosis AS SELECT
    ref_diagnosis.diag_code,
    ref_diagnosis.diag_type_code,
    ref_diagnosis.drg_major_diag_cat_code,
    ref_diagnosis.diagnosis_desc,
    ref_diagnosis.diagnosis_short_desc,
    ref_diagnosis.eff_from_date,
    ref_diagnosis.eff_to_date,
    ref_diagnosis.sex_edit_indicator,
    ref_diagnosis.external_cause_ind,
    ref_diagnosis.source_system_code
  FROM
    {{ params.param_pf_base_views_dataset_name }}.ref_diagnosis
;
