-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/sma_patient_exhibits.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.sma_patient_exhibits AS SELECT
    ROUND(sma_patient_exhibits.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    sma_patient_exhibits.rptg_period,
    sma_patient_exhibits.company_code,
    sma_patient_exhibits.coid,
    ROUND(sma_patient_exhibits.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    sma_patient_exhibits.iplan_id_ins1,
    sma_patient_exhibits.exhibit_num,
    ROUND(sma_patient_exhibits.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    ROUND(sma_patient_exhibits.add_accrual_needed_amt, 3, 'ROUND_HALF_EVEN') AS add_accrual_needed_amt,
    ROUND(sma_patient_exhibits.override_pct, 5, 'ROUND_HALF_EVEN') AS override_pct,
    ROUND(sma_patient_exhibits.override_amt, 3, 'ROUND_HALF_EVEN') AS override_amt,
    sma_patient_exhibits.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.sma_patient_exhibits
;
