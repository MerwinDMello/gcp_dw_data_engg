-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cancer_tumor_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cancer_tumor_driver AS SELECT
    a.cancer_tumor_driver_sk,
    a.cp_icd_oncology_code,
    a.cp_icd_oncology_site_desc,
    a.cp_icd_oncology_group_name,
    a.cr_tumor_primary_site_id,
    a.cn_tumor_type_id,
    a.cn_general_tumor_type_id,
    a.cn_navque_tumor_type_id,
    a.cr_icd_oncology_code,
    a.cr_icd_oncology_site_desc,
    a.cn_tumor_group_name,
    a.cn_tumor_type_desc,
    a.cn_general_tumor_group_name,
    a.cn_general_tumor_type_desc,
    a.cn_navque_tumor_type_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_tumor_driver AS a
;
