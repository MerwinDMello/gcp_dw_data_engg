-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_radiation_oncology.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.cn_patient_rad_oncology_sid,
          loc.site_location_id,
          stg.treatment_type_id,
          lob_loc.lung_lobe_location_id,
          fac.facility_id,
          stg.nav_patient_id,
          stg.core_record_type_id,
          stg.med_spcl_physician_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.core_record_date,
          stg.treatment_start_date,
          stg.treatment_end_date,
          stg.treatment_fractions_num,
          stg.elapse_ind,
          stg.elapse_start_date,
          stg.elapse_end_date,
          stg.radiation_oncology_reason_text,
          stg.palliative_ind,
          stg.treatment_therapy_schedule_cd,
          stg.comment_text,
          stg.hashbite_ssk,
          stg.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_radiation_oncology_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_site_location AS loc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.treatment_site_location_id)) = upper(rtrim(loc.site_location_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_lung_lobe_location AS lob_loc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.lung_lobe_location_id)) = upper(rtrim(lob_loc.lung_lobe_location_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_facility AS fac
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.radiation_oncology_facility_id)) = upper(rtrim(fac.facility_name))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_radiation_oncology.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_radiation_oncology
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS a