-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_timeline.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT cn_patient_timeline_stg.cn_patient_timeline_id,
          cn_patient_timeline_stg.nav_patient_id,
          cn_patient_timeline_stg.tumor_type_id,
          cn_patient_timeline_stg.navigator_id,
          cn_patient_timeline_stg.coid,
          cn_patient_timeline_stg.company_code,
          cn_patient_timeline_stg.nav_referred_date,
          cn_patient_timeline_stg.first_treatment_date,
          cn_patient_timeline_stg.first_consult_date,
          cn_patient_timeline_stg.first_imaging_date,
          cn_patient_timeline_stg.first_medical_oncology_date,
          cn_patient_timeline_stg.first_radiation_oncology_date,
          cn_patient_timeline_stg.first_diagnosis_date,
          cn_patient_timeline_stg.first_biopsy_date,
          cn_patient_timeline_stg.first_surgery_consult_date,
          cn_patient_timeline_stg.first_surgery_date,
          cn_patient_timeline_stg.surv_care_plan_close_date,
          cn_patient_timeline_stg.surv_care_plan_resolve_date,
          cn_patient_timeline_stg.end_treatment_date,
          cn_patient_timeline_stg.death_date,
          cn_patient_timeline_stg.diag_first_trt_day_num,
          cn_patient_timeline_stg.diag_first_trt_available_ind,
          cn_patient_timeline_stg.hashbite_ssk,
          cn_patient_timeline_stg.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_timeline_stg
   WHERE upper(cn_patient_timeline_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_timeline.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_timeline) ) AS a