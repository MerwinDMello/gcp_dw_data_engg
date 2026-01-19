CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_mltp_disciplinary_meeting
   OPTIONS(description='Contains the details behind any meetings of physicians, navigators, specialists around a patient.')
  AS SELECT
      cn_patient_mltp_disciplinary_meeting.cn_patient_mltp_disciplinary_meet_sid,
      cn_patient_mltp_disciplinary_meeting.nav_patient_id,
      cn_patient_mltp_disciplinary_meeting.tumor_type_id,
      cn_patient_mltp_disciplinary_meeting.navigator_id,
      cn_patient_mltp_disciplinary_meeting.coid,
      cn_patient_mltp_disciplinary_meeting.company_code,
      cn_patient_mltp_disciplinary_meeting.meeting_date,
      cn_patient_mltp_disciplinary_meeting.patient_discussed_ind,
      cn_patient_mltp_disciplinary_meeting.treatment_change_ind,
      cn_patient_mltp_disciplinary_meeting.meeting_notes_text,
      cn_patient_mltp_disciplinary_meeting.hashbite_ssk,
      cn_patient_mltp_disciplinary_meeting.source_system_code,
      cn_patient_mltp_disciplinary_meeting.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_mltp_disciplinary_meeting
  ;
