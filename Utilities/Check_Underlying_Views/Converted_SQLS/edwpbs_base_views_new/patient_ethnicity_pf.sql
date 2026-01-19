-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_ethnicity_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_ethnicity_pf AS SELECT
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.patient_ethnicity_type_code,
    a.pat_acct_num,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.patient_ethnicity AS a
;
