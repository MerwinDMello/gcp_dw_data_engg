-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cdm_lab_result_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cdm_lab_result_detail AS SELECT
    a.clinical_finding_sk,
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.lab_test_type_ref_code,
    a.lab_test_type_desc,
    a.lab_test_id_text,
    a.lab_test_subid_text,
    a.lab_test_collect_ts,
    a.lab_test_specimen_received_ts,
    a.lab_test_reported_ts,
    a.lab_test_result_status_ts,
    a.lab_test_result_status_ref_code,
    a.lab_test_value_numeric_ind,
    a.lab_test_value_unit_type_code,
    a.lab_test_value_text,
    a.lab_test_value_num,
    a.ref_range_low_text,
    a.ref_range_high_text,
    a.ref_range_text,
    a.abnormal_flag_name,
    a.source_system_original_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cdm_lab_result_detail AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
