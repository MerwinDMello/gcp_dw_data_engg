-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_mt_cl_user_activity.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT clinical_user_patient_audit.company_code AS company_code,
          clinical_user_patient_audit.coid AS coid,
          upper(clinical_user_patient_audit.audit_event_user_mnem_cs) AS user_mnemonic,
          clinical_user_patient_audit.network_mnemonic_cs AS network_mnemonic_cs,
          DATE(clinical_user_patient_audit.audit_event_date_time) AS mt_activity_date,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM {{ params.param_im_auth_base_views_dataset_name }}.clinical_user_patient_audit QUALIFY row_number() OVER (PARTITION BY network_mnemonic_cs,
                                                                                                                               user_mnemonic
                                                                                                                  ORDER BY mt_activity_date DESC) = 1) AS a