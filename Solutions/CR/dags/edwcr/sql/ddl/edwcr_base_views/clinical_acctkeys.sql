CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.clinical_acctkeys AS SELECT
    clinical_acctkeys.coid,
    clinical_acctkeys.company_code,
    clinical_acctkeys.unit_num,
    clinical_acctkeys.patient_dw_id,
    clinical_acctkeys.pat_acct_num
  FROM
    {{ params.param_pf_staging_dataset_name }}.clinical_acctkeys
;
