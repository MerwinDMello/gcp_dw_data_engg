-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cn_patient_mltp_disciplinary_meeting.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cn_patient_mltp_disciplinary_meeting AS SELECT
    a.cn_patient_mltp_disciplinary_meet_sid,
    a.nav_patient_id,
    a.tumor_type_id,
    a.navigator_id,
    a.coid,
    a.company_code,
    a.meeting_date,
    a.patient_discussed_ind,
    a.treatment_change_ind,
    a.meeting_notes_text,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_mltp_disciplinary_meeting AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
