-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_cancer_pat_id_input_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY sk.cancer_patient_driver_sk) AS cancer_patient_id_input_driver_sk,
                            sk.cancer_patient_driver_sk,
                            a.message_control_id_text,
                            a.patient_type_status_sk,
                            a.coid,
                            a.company_code,
                            a.patient_dw_id,
                            a.pat_acct_num,
                            a.medical_record_num,
                            a.patient_market_urn,
                            a.message_type_code,
                            a.message_flag_code,
                            a.message_event_type_code,
                            a.message_origin_requested_date_time,
                            a.message_signed_observation_date_time,
                            a.message_created_date_time,
                            a.first_insert_date_time,
                            'H' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_input AS a
   LEFT OUTER JOIN
     (SELECT a1.cancer_patient_driver_sk,
             a1.message_control_id_text
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output_driver AS a1 QUALIFY row_number() OVER (PARTITION BY upper(a1.message_control_id_text)
                                                                                                                   ORDER BY a1.user_action_date_time DESC) = 1) AS sk ON upper(rtrim(a.message_control_id_text)) = upper(rtrim(sk.message_control_id_text))) AS stg