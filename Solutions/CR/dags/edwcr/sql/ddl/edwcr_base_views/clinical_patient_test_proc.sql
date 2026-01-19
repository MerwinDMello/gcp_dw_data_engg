CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.clinical_patient_test_proc AS SELECT
    clinical_patient_test_proc.patient_dw_id,
    clinical_patient_test_proc.specimen_urn,
    clinical_patient_test_proc.clinical_proc_num,
    clinical_patient_test_proc.company_code,
    clinical_patient_test_proc.coid,
    clinical_patient_test_proc.pat_acct_num,
    clinical_patient_test_proc.clinical_proc_order_date,
    clinical_patient_test_proc.clinical_proc_order_time,
    clinical_patient_test_proc.clinical_proc_order_entry_date,
    clinical_patient_test_proc.clinical_proc_order_entry_time,
    clinical_patient_test_proc.clinical_proc_num_ancestor,
    clinical_patient_test_proc.clinical_proc_profile_mnem_cs,
    clinical_patient_test_proc.clinical_system_module_code,
    clinical_patient_test_proc.network_mnemonic_cs,
    clinical_patient_test_proc.facility_mnemonic_cs,
    clinical_patient_test_proc.source_system_code,
    clinical_patient_test_proc.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.clinical_patient_test_proc
;
