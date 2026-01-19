CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_icd_oncology
   OPTIONS(description='The International Classification of Diseases for Oncology (ICD-O) has been internationally recognized as the definitive classification of neoplasms. It is used by cancer registries throughout the world to record incidence of malignancy and survival rates.')
  AS SELECT
      ref_icd_oncology.icd_oncology_code,
      ref_icd_oncology.icd_oncology_type_code,
      ref_icd_oncology.icd_oncology_category_type_code,
      ref_icd_oncology.icd_oncology_site_desc,
      ref_icd_oncology.source_system_code,
      ref_icd_oncology.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_icd_oncology
  ;
