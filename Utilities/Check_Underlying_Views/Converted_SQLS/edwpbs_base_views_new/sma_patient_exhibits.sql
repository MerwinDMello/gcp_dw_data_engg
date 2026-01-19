-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/sma_patient_exhibits.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.sma_patient_exhibits AS SELECT
    sma_patient_exhibits.patient_dw_id,
    sma_patient_exhibits.rptg_period,
    sma_patient_exhibits.company_code,
    sma_patient_exhibits.coid,
    sma_patient_exhibits.pat_acct_num,
    sma_patient_exhibits.iplan_id_ins1,
    sma_patient_exhibits.exhibit_num,
    sma_patient_exhibits.financial_class_code,
    sma_patient_exhibits.add_accrual_needed_amt,
    sma_patient_exhibits.override_pct,
    sma_patient_exhibits.override_amt,
    sma_patient_exhibits.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.sma_patient_exhibits
;
