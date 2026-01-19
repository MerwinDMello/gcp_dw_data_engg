-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/surgical_case_schedule_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.surgical_case_schedule_detail AS SELECT
    a.srg_case_sk,
    a.surgical_case_id_text,
    a.surgical_appt_id_text,
    a.patient_dw_id,
    a.pat_acct_num,
    a.company_code,
    a.coid,
    a.same_store_sw,
    a.sub_unit_num,
    a.unit_num,
    a.surgical_location_desc,
    a.medical_record_num,
    a.patient_type_code,
    a.case_verification_status_text,
    a.room_text,
    a.room_type_code,
    a.case_schedule_date_time,
    a.case_entered_date_time,
    a.patient_in_date_time,
    a.case_cancel_date_time,
    a.planned_sw,
    a.cancelled_sw,
    a.reschedule_ind,
    a.event_cancel_reason_text,
    a.event_cancel_desc,
    a.er_sw,
    a.proposed_surgeon_mnemonic_cs,
    a.proposed_surgeon_facility_physician_num,
    a.proposed_surgeon_full_name,
    a.proposed_surgeon_npi,
    a.proposed_surgeon_taxonomy_code,
    a.proposed_primary_procedure_ind,
    a.proposed_procedure_mnemonic_cs,
    a.proposed_procedure_desc,
    a.proposed_procedure_group_desc,
    a.proposed_procedure_group_exclusion_ind,
    a.proposed_procedure_duration_amt,
    a.proposed_cpt_code,
    a.proposed_esl_level_2_desc,
    a.proposed_esl_level_3_desc,
    a.proposed_esl_level_4_desc,
    a.patient_full_name,
    a.insurance_mnemonic_cs,
    a.iplan_id,
    a.major_payor_group_id,
    a.financial_class_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.surgical_case_schedule_detail AS a
;
