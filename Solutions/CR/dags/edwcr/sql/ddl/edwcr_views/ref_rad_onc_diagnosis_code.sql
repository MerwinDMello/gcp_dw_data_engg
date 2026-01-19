-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/ref_rad_onc_diagnosis_code.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.ref_rad_onc_diagnosis_code AS SELECT
    a.diagnosis_code_sk,
    a.site_sk,
    a.source_diagnosis_code_id,
    a.diagnosis_code,
    a.diagnosis_site_text,
    a.diagnosis_code_class_schema_id,
    a.diagnosis_clinical_desc,
    a.diagnosis_long_desc,
    a.diagnosis_type_code,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_diagnosis_code AS a
;
