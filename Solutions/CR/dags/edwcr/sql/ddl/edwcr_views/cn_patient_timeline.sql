-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cn_patient_timeline.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cn_patient_timeline AS SELECT
    a.cn_patient_timeline_id,
    a.nav_patient_id,
    a.tumor_type_id,
    a.navigator_id,
    a.coid,
    a.company_code,
    a.nav_referred_date,
    a.first_treatment_date,
    a.first_consult_date,
    a.first_imaging_date,
    a.first_medical_oncology_date,
    a.first_radiation_oncology_date,
    a.first_diagnosis_date,
    a.first_biopsy_date,
    a.first_surgery_consult_date,
    a.first_surgery_date,
    a.survivorship_care_plan_close_date,
    a.survivorship_care_plan_resolve_date,
    a.end_treatment_date,
    a.death_date,
    a.diagnosis_first_treatment_day_num,
    a.diagnosis_first_treatment_available_ind,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_timeline AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
