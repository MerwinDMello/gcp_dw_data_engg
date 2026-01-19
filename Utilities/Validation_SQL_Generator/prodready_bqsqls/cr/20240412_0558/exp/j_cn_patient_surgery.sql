-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_surgery.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.cn_patient_surgery_sid,
          rs.side_id,
          rf.facility_id,
          stg.surgery_type_id,
          stg.core_record_type_id,
          stg.med_spcl_physician_id,
          stg.referring_physician_id,
          stg.nav_patient_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.surgery_date,
          stg.general_surgery_type_text,
          stg.reconstructive_offered_ind,
          stg.palliative_ind,
          stg.comment_text,
          stg.hashbite_ssk,
          stg.source_system_code,
          stg.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_surgery_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_side AS rs
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.surgeryside)) = upper(trim(rs.side_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_facility AS rf
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.surgeryfacility)) = upper(trim(rf.facility_name))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_surgery.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_surgery
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS a