-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cancer_patient_id_output_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cancer_patient_id_output_driver AS SELECT
    a.cancer_patient_id_output_driver_sk,
    a.cancer_patient_driver_sk,
    a.cancer_patient_id_output_sk,
    a.message_control_id_text,
    a.user_action_date_time,
    a.coid,
    a.company_code,
    a.patient_dw_id,
    a.pat_acct_num,
    a.medical_record_num,
    a.patient_market_urn,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.last_name
    END AS last_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.first_name
    END AS first_name,
    a.birth_date,
    a.gender_code,
    CASE
      WHEN session_user() = so.userid THEN a.social_security_num
      ELSE '***'
    END AS social_security_num,
    a.address_line_1_text,
    a.address_line_2_text,
    a.city_name,
    a.state_code,
    a.zip_code,
    a.patient_type_code,
    a.message_type_code,
    a.message_event_type_code,
    a.message_flag_code,
    a.message_origin_requested_date_time,
    a.message_signed_observation_date_time,
    a.message_ingestion_date_time,
    a.message_created_date_time,
    a.document_type_id_text,
    a.document_type_text,
    a.document_type_coding_system_code,
    a.first_insert_date_time,
    a.icd_oncology_type_code,
    a.predicted_primary_icd_oncology_code,
    a.predicted_primary_icd_site_desc,
    a.suggested_primary_icd_oncology_code,
    a.suggested_primary_icd_site_desc,
    a.submitted_primary_icd_oncology_code,
    a.submitted_primary_icd_site_desc,
    a.transition_of_care_num,
    a.user_action_desc,
    a.user_action_criticality_text,
    a.user_3_4_id,
    a.report_assigned_date_time,
    a.attending_physician_full_name,
    a.pcp_full_name,
    a.pcp_phone_num,
    a.facility_menmonic_cs,
    a.network_mnemonic_cs,
    a.model_predicted_sarcoma_ind,
    a.submitted_sarcoma_ind,
    a.suggested_sarcoma_ind,
    a.benign_brain_tumor_type_ind,
    a.met_to_brain_tumor_type_ind,
    a.other_tumor_type_ind,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output_driver AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          {{ params.param_auth_base_views_dataset_name }}.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'PN'
    ) AS pn ON pn.userid = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          {{ params.param_auth_base_views_dataset_name }}.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'SSN'
    ) AS so ON so.userid = session_user()
;
