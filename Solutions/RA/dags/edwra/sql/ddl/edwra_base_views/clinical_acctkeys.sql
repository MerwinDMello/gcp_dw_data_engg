-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/clinical_acctkeys.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.clinical_acctkeys AS SELECT
    clinical_acctkeys.coid,
    clinical_acctkeys.company_code,
    clinical_acctkeys.unit_num,
    clinical_acctkeys.patient_dw_id,
    clinical_acctkeys.pat_acct_num
  FROM
    {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys
;
