-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_id_output_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_id_output_driver AS SELECT
    a1.cancer_patient_id_output_driver_sk,
    a1.cancer_patient_driver_sk,
    a1.cancer_patient_id_output_sk,
    a1.message_control_id_text,
    a1.user_action_date_time,
    a1.coid,
    a1.company_code,
    a1.patient_dw_id,
    a1.pat_acct_num,
    a1.medical_record_num,
    a1.patient_market_urn,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a1.last_name
    END AS last_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a1.first_name
    END AS first_name,
    a1.birth_date,
    a1.gender_code,
    CASE
      WHEN session_user() = so.userid THEN a1.social_security_num
      ELSE '***'
    END AS social_security_num,
    a1.address_line_1_text,
    a1.address_line_2_text,
    a1.city_name,
    a1.state_code,
    a1.zip_code,
    a1.patient_type_code,
    a1.message_type_code,
    a1.message_event_type_code,
    a1.message_flag_code,
    a1.message_origin_requested_date_time,
    a1.message_signed_observation_date_time,
    a1.message_ingestion_date_time,
    a1.message_created_date_time,
    a1.document_type_id_text,
    a1.document_type_text,
    a1.document_type_coding_system_code,
    a1.first_insert_date_time,
    a1.icd_oncology_type_code,
    a1.predicted_primary_icd_oncology_code,
    a1.predicted_primary_icd_site_desc,
    a1.suggested_primary_icd_oncology_code,
    a1.suggested_primary_icd_site_desc,
    a1.submitted_primary_icd_oncology_code,
    a1.submitted_primary_icd_site_desc,
    a1.transition_of_care_num,
    a1.user_action_desc,
    a1.user_action_criticality_text,
    a1.user_3_4_id,
    a1.report_assigned_date_time,
    a1.attending_physician_full_name,
    a1.pcp_full_name,
    a1.pcp_phone_num,
    a1.facility_menmonic_cs,
    a1.network_mnemonic_cs,
    a1.model_predicted_sarcoma_ind,
    a1.submitted_sarcoma_ind,
    a1.suggested_sarcoma_ind,
    a1.source_system_code,
    a1.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cancer_patient_id_output_driver AS a1
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a1.company_code = b.company_code
     AND a1.coid = b.co_id
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
