-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cdm_patient_medication.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cdm_patient_medication AS SELECT
    a.medication_admn_sk,
    a.patient_dw_id,
    a.coid,
    a.company_code,
    a.medication_desc,
    a.occurence_ts,
    a.drug_dose_amt_text,
    a.drug_dose_measurement_text,
    a.administrative_frequency_text,
    a.administered_unit_cnt,
    a.route_code_sk,
    a.route_code_desc,
    a.ordering_physician_name,
    a.physician_npi,
    a.medication_num_text,
    a.source_system_original_code,
    a.clinical_pharmacy_trade_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cdm_patient_medication AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
