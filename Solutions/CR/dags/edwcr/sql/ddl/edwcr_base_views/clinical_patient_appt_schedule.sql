CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.clinical_patient_appt_schedule AS SELECT
    clinical_patient_appt_schedule.patient_dw_id,
    clinical_patient_appt_schedule.appointment_urn,
    clinical_patient_appt_schedule.company_code,
    clinical_patient_appt_schedule.coid,
    clinical_patient_appt_schedule.pat_acct_num,
    clinical_patient_appt_schedule.appointment_date_time,
    clinical_patient_appt_schedule.appointment_made_date,
    clinical_patient_appt_schedule.appointment_type,
    clinical_patient_appt_schedule.appointment_status,
    clinical_patient_appt_schedule.receiving_facility_mnemonic_cs,
    clinical_patient_appt_schedule.receiving_location_mnemonic_cs,
    clinical_patient_appt_schedule.resource_group_mnemonic_cs,
    clinical_patient_appt_schedule.clinical_resource_mnemonic_cs,
    clinical_patient_appt_schedule.appointment_duration_min_cnt,
    clinical_patient_appt_schedule.surgeon_mnemonic_cs,
    clinical_patient_appt_schedule.adm_phy_mnemonic_cs,
    clinical_patient_appt_schedule.resource_reserved_date_time,
    clinical_patient_appt_schedule.case_type_code,
    clinical_patient_appt_schedule.morning_admit_ind,
    clinical_patient_appt_schedule.network_mnemonic_cs,
    clinical_patient_appt_schedule.facility_mnemonic_cs,
    clinical_patient_appt_schedule.source_system_code,
    clinical_patient_appt_schedule.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.clinical_patient_appt_schedule
;
