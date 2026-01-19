-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/rad_onc_patient.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.rad_onc_patient AS SELECT
    a.patient_sk,
    a.patient_address_sk,
    a.site_sk,
    a.source_patient_id,
    a.medical_record_num,
    a.patient_birth_date_time,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_first_name
    END AS patient_first_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_middle_name
    END AS patient_middle_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_last_name
    END AS patient_last_name,
    a.patient_title_name,
    a.patient_email_address_text,
    a.patient_in_out_ind,
    a.patient_death_ind,
    a.patient_death_date,
    a.patient_death_reason_text,
    a.clinical_trial_ind,
    a.patient_transportation_text,
    a.patient_global_unique_id_text,
    a.patient_room_number_text,
    a.active_ind,
    a.patient_language_text,
    a.patient_notes_text,
    a.log_id,
    a.run_id,
    a.history_user_name,
    a.history_date_time,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient AS a
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          {{ params.param_auth_base_views_dataset_name }}.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'PN'
    ) AS pn ON pn.userid = session_user()
;
