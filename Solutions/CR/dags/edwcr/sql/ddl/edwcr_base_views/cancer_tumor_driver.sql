CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_tumor_driver
   OPTIONS(description='Master tumor table which stores tumor types (International Classification of Diseases for Oncology ) from cancer registry , cancer patient id and cancer navigation system')
  AS SELECT
      cancer_tumor_driver.cancer_tumor_driver_sk,
      cancer_tumor_driver.cp_icd_oncology_code,
      cancer_tumor_driver.cp_icd_oncology_site_desc,
      cancer_tumor_driver.cp_icd_oncology_group_name,
      cancer_tumor_driver.cr_tumor_primary_site_id,
      cancer_tumor_driver.cn_tumor_type_id,
      cancer_tumor_driver.cn_general_tumor_type_id,
      cancer_tumor_driver.cn_navque_tumor_type_id,
      cancer_tumor_driver.cr_icd_oncology_code,
      cancer_tumor_driver.cr_icd_oncology_site_desc,
      cancer_tumor_driver.cn_tumor_group_name,
      cancer_tumor_driver.cn_tumor_type_desc,
      cancer_tumor_driver.cn_general_tumor_group_name,
      cancer_tumor_driver.cn_general_tumor_type_desc,
      cancer_tumor_driver.cn_navque_tumor_type_desc,
      cancer_tumor_driver.source_system_code,
      cancer_tumor_driver.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver
  ;
