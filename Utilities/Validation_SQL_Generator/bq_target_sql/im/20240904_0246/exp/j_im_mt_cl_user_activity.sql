-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_cl_user_activity.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(count(*), 0)) AS source_string
  FROM
    (
      SELECT
          clinical_user_patient_audit.company_code AS company_code,
          clinical_user_patient_audit.coid AS coid,
          upper(clinical_user_patient_audit.audit_event_user_mnem_cs) AS user_mnemonic,
          clinical_user_patient_audit.network_mnemonic_cs AS network_mnemonic_cs,
          DATE(clinical_user_patient_audit.audit_event_date_time) AS mt_activity_date,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
        FROM
          `hca-hin-dev-cur-comp`.edwim_base_views.clinical_user_patient_audit
        QUALIFY row_number() OVER (PARTITION BY network_mnemonic_cs, user_mnemonic ORDER BY mt_activity_date DESC) = 1
    ) AS a
;
