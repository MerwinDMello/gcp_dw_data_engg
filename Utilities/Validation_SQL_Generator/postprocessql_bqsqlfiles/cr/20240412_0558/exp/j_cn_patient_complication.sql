-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_complication.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT stg.cn_patient_complication_sid,
          stg.nav_patient_id,
          stg.core_record_type_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          'H' AS company_code,
          stg.complication_date,
          rtt.therapy_type_id,
          stg.treatment_stopped_ind,
          rs.nav_result_id AS outcome_result_id,
          stg.complication_text,
          stg.specific_complication_text,
          stg.comment_text,
          stg.hashbite_ssk,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_complication_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_therapy_type AS rtt ON upper(rtrim(coalesce(trim(stg.associatetherapytype), 'X'))) = upper(rtrim(coalesce(trim(rtt.therapy_type_desc), 'X')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_result AS rs ON upper(rtrim(coalesce(trim(stg.complicationoutcome), 'XX'))) = upper(rtrim(coalesce(trim(rs.nav_result_desc), 'XX')))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_complication.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_complication) ) AS src