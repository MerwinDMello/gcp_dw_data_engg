-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cdm_patient.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cdm_patient AS SELECT
    a.role_plyr_sk,
    a.empi_text,
    a.patient_race_code_sk,
    a.patient_race_desc,
    a.address_line_1_text,
    a.address_line_2_text,
    a.city_name,
    a.state_code,
    a.zip_code,
    a.home_phone_num,
    a.business_phone_num,
    a.mobile_phone_num,
    a.birth_date_time,
    a.death_date_time,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.first_name
    END AS first_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.middle_name
    END AS middle_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.last_name
    END AS last_name,
    a.gender_code,
    a.patient_email_text,
    a.vital_status_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cdm_patient AS a
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
