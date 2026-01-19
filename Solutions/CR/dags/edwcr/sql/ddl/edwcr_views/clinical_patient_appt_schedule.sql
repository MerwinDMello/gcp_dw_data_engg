-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/clinical_patient_appt_schedule.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.clinical_patient_appt_schedule AS SELECT
    a.patient_dw_id,
    a.appointment_urn,
    a.company_code,
    a.coid,
    a.pat_acct_num,
    a.appointment_date_time,
    a.appointment_made_date,
    a.appointment_type,
    a.appointment_status,
    a.receiving_facility_mnemonic_cs,
    a.receiving_location_mnemonic_cs,
    a.resource_group_mnemonic_cs,
    a.clinical_resource_mnemonic_cs,
    a.appointment_duration_min_cnt,
    a.surgeon_mnemonic_cs,
    a.adm_phy_mnemonic_cs,
    a.resource_reserved_date_time,
    a.case_type_code,
    a.morning_admit_ind,
    a.network_mnemonic_cs,
    a.facility_mnemonic_cs,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.clinical_patient_appt_schedule AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
