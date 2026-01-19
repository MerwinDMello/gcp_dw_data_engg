-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/clinical_acctkeys.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.clinical_acctkeys AS SELECT
    clinical_acctkeys.coid,
    clinical_acctkeys.company_code,
    clinical_acctkeys.unit_num,
    ROUND(clinical_acctkeys.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(clinical_acctkeys.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num
  FROM
    {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys
;
