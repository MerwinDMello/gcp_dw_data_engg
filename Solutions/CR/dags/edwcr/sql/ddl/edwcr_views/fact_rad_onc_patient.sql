-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_rad_onc_patient.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_rad_onc_patient AS SELECT
    a.fact_patient_sk,
    a.hospital_sk,
    a.patient_sk,
    a.patient_status_id,
    a.location_sk,
    a.race_id,
    a.gender_id,
    a.site_sk,
    a.source_fact_patient_id,
    a.creation_date_time,
    a.admission_date_time,
    a.discharge_date_time,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_patient AS a
;
