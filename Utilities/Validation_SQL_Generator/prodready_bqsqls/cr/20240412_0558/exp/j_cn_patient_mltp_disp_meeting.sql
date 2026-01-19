-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_mltp_disp_meeting.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.cn_patient_mltp_disc_meet_sid,
          stg.nav_patient_id,
          stg.tumor_type_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.meeting_date,
          stg.patient_discussed_ind,
          stg.treatment_change_ind,
          stg.meeting_notes_text,
          stg.hashbite_ssk,
          stg.source_system_code,
          stg.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_mltp_disciplinary_meeting_stg AS stg
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_mltp_disciplinary_meeting.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_mltp_disciplinary_meeting
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS a