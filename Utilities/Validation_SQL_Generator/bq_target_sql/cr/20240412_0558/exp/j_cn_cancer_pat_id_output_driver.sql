-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_cancer_pat_id_output_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY a1.cancer_patient_driver_sk, a1.cancer_patient_id_output_sk) AS cancer_patient_id_output_driver_sk,
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
          a1.last_name,
          a1.first_name,
          a1.birth_date,
          a1.gender_code,
          a1.social_security_num,
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
          (
            SELECT DISTINCT
                cpd.cancer_patient_driver_sk,
                a.cancer_patient_id_output_sk,
                a.message_control_id_text,
                a.user_action_date_time,
                a.coid,
                a.company_code,
                a.patient_dw_id,
                a.pat_acct_num,
                a.medical_record_num,
                a.patient_market_urn,
                a.last_name,
                a.first_name,
                a.birth_date,
                a.gender_code,
                a.social_security_num,
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
                a.source_system_code,
                a.dw_last_update_date_time
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS a
                INNER JOIN (
                  SELECT
                      cancer_patient_driver.cancer_patient_driver_sk,
                      cancer_patient_driver.coid,
                      cancer_patient_driver.cp_patient_id,
                      cancer_patient_driver.network_mnemonic_cs
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver
                    QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cp_patient_id, cancer_patient_driver.network_mnemonic_cs ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1
                ) AS cpd ON a.patient_dw_id = cpd.cp_patient_id
                 AND rtrim(a.network_mnemonic_cs) = rtrim(cpd.network_mnemonic_cs)
            UNION DISTINCT
            SELECT DISTINCT
                cpd.cancer_patient_driver_sk,
                a.cancer_patient_id_output_sk,
                a.message_control_id_text,
                a.user_action_date_time,
                coalesce(a.coid, df2.coid) AS coid,
                a.company_code,
                a.patient_dw_id,
                a.pat_acct_num,
                a.medical_record_num,
                a.patient_market_urn,
                a.last_name,
                a.first_name,
                a.birth_date,
                a.gender_code,
                a.social_security_num,
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
                a.source_system_code,
                a.dw_last_update_date_time
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS a
                LEFT OUTER JOIN (
                  SELECT
                      clinical_facility.facility_mnemonic_cs,
                      clinical_facility.coid,
                      clinical_facility.facility_active_ind
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.clinical_facility
                    QUALIFY row_number() OVER (PARTITION BY clinical_facility.facility_mnemonic_cs ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1
                ) AS df2 ON rtrim(a.facility_menmonic_cs) = rtrim(df2.facility_mnemonic_cs)
                LEFT OUTER JOIN (
                  SELECT
                      cancer_patient_driver.cancer_patient_driver_sk,
                      cancer_patient_driver.coid,
                      coalesce(concat(cancer_patient_driver.patient_market_urn_text, cancer_patient_driver.network_mnemonic_cs), concat(cancer_patient_driver.medical_record_num, cancer_patient_driver.coid)) AS pid,
                      cancer_patient_driver.network_mnemonic_cs
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver
                    QUALIFY row_number() OVER (PARTITION BY pid ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1
                ) AS cpd ON rtrim(coalesce(concat(a.patient_market_urn, a.network_mnemonic_cs), concat(a.medical_record_num, coalesce(a.coid, df2.coid)))) = rtrim(cpd.pid)
                 AND rtrim(coalesce(a.network_mnemonic_cs, '-1')) = rtrim(coalesce(cpd.network_mnemonic_cs, '-1'))
              WHERE (coalesce(a.patient_dw_id, NUMERIC '0'), a.network_mnemonic_cs) NOT IN(
                SELECT AS STRUCT
                    coalesce(a_0.patient_dw_id, NUMERIC '0'),
                    a_0.network_mnemonic_cs
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS a_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS cpd_0 ON a_0.patient_dw_id = cpd_0.cp_patient_id
                     AND rtrim(a_0.network_mnemonic_cs) = rtrim(cpd_0.network_mnemonic_cs)
              )
          ) AS a1
    ) AS stg
;
