CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_tumor_site_diagnosis
   OPTIONS(description='Contains site of tumor based on diagnosis code.')
  AS SELECT
      ref_tumor_site_diagnosis.diagnosis_code,
      ref_tumor_site_diagnosis.diagnosis_type_code,
      ref_tumor_site_diagnosis.eff_from_date,
      ref_tumor_site_diagnosis.eff_to_date,
      ref_tumor_site_diagnosis.tumor_site_id,
      ref_tumor_site_diagnosis.detail_tumor_site_id,
      ref_tumor_site_diagnosis.tumor_type_id,
      ref_tumor_site_diagnosis.source_system_code,
      ref_tumor_site_diagnosis.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_tumor_site_diagnosis
  ;
