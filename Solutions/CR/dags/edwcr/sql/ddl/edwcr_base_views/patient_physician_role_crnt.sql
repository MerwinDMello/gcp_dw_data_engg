CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.patient_physician_role_crnt AS SELECT
    patient_physician_role_crnt.patient_dw_id,
    patient_physician_role_crnt.role_type_code,
    patient_physician_role_crnt.company_code,
    patient_physician_role_crnt.coid,
    patient_physician_role_crnt.unit_num,
    patient_physician_role_crnt.pat_acct_num,
    patient_physician_role_crnt.patient_type_code,
    patient_physician_role_crnt.facility_physician_num,
    patient_physician_role_crnt.source_system_code,
    patient_physician_role_crnt.dw_last_update_date_time
  FROM
    {{ params.param_pf_base_views_dataset_name }}.patient_physician_role_crnt
;
