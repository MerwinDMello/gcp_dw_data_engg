-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cr_patient_medical_oncology.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cr_patient_medical_oncology AS SELECT
    a.drug_id,
    a.treatment_id,
    a.cycle_id,
    a.drug_route_id,
    a.drug_dose_unit_id,
    a.drug_hospital_id,
    a.nsc_id,
    a.total_drug_dose_amt,
    a.drug_days_given_num_text,
    a.drug_frequency_num,
    a.treatment_start_date,
    a.treatment_end_date,
    a.cycle_num_text,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cr_patient_medical_oncology AS a
;
