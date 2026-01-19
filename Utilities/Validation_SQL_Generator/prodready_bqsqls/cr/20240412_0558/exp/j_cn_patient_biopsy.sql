-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_biopsy.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.cn_patient_biopsy_sid,
          stg.core_record_type_id,
          stg.med_spcl_physician_id,
          stg.referring_physician_id,
          stg.biopsy_type_id,
          stg.biopsy_result_id,
          fac.facility_id,
          l.site_location_id,
          s.physician_specialty_id,
          stg.nav_patient_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.biopsy_date,
          stg.biopsy_clip_sw,
          stg.biopsy_needle_sw,
          stg.general_biopsy_type_text,
          stg.comment_text,
          stg.hashbite_ssk,
          stg.source_system_code,
          stg.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_biopsy_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS fac ON upper(rtrim(stg.biopsyfacility)) = upper(rtrim(fac.facility_name))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_site_location AS l ON upper(rtrim(stg.biopsysite)) = upper(rtrim(l.site_location_desc))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_physician_specialty AS s ON upper(rtrim(stg.biopsyphysiciantype)) = upper(rtrim(s.physician_specialty_desc))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_biopsy.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_base_views_dataset_name }}.cn_patient_biopsy) ) AS a