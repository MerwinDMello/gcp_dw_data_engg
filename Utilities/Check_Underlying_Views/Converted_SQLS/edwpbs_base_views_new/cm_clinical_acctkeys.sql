-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/cm_clinical_acctkeys.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.cm_clinical_acctkeys AS SELECT
    clinical_acctkeys.coid,
    clinical_acctkeys.company_code,
    clinical_acctkeys.unit_num,
    clinical_acctkeys.patient_dw_id,
    clinical_acctkeys.pat_acct_num
  FROM
    `hca-hin-dev-cur-parallon`.edwcm_base_views.clinical_acctkeys
;
