-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cdm_encounter.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cdm_encounter AS SELECT
    a.patient_dw_id,
    a.pat_acct_num,
    a.coid,
    a.company_code,
    a.patient_sk,
    a.facility_sk,
    a.medical_record_num,
    a.patient_market_urn,
    a.arrival_mode_sk,
    a.arrival_mode_code,
    a.arrival_mode_desc,
    a.admit_source_sk,
    a.admit_source_code,
    a.admit_source_desc,
    a.admit_type_code,
    a.visit_type_sk,
    a.visit_type_code,
    a.visit_type_desc,
    a.special_program_sk,
    a.special_program_code,
    a.special_program_desc,
    a.discharge_status_sk,
    a.discharge_status_code,
    a.discharge_status_desc,
    a.encounter_date_time,
    a.admission_date_time,
    a.accident_date_time,
    a.discharge_date_time,
    a.reason_for_visit_text,
    a.actual_los_cnt,
    a.signature_date,
    a.readmission_ind,
    a.source_system_text,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cdm_encounter AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
