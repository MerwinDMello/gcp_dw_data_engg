-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_family_history.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT cn_patient_family_history_stg.patienthistoryfactid AS cn_patient_family_history_sid,
          cn_patient_family_history_stg.family_history_query_id,
          cn_patient_family_history_stg.patientdimid AS nav_patient_id,
          cn_patient_family_history_stg.coid,
          'H' AS company_code,
          cn_patient_family_history_stg.family_history_value_text,
          cn_patient_family_history_stg.hbsource AS hashbite_ssk,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_family_history_stg
   WHERE upper(cn_patient_family_history_stg.hbsource) NOT IN
       (SELECT upper(cn_patient_family_history.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_base_views_dataset_name }}.cn_patient_family_history) ) AS src