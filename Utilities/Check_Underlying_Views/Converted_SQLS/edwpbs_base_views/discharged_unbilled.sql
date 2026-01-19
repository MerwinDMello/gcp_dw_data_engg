-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/discharged_unbilled.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.discharged_unbilled AS SELECT
    discharged_unbilled.coid,
    discharged_unbilled.company_code,
    ROUND(discharged_unbilled.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    discharged_unbilled.effective_date,
    discharged_unbilled.unit_num,
    ROUND(discharged_unbilled.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    discharged_unbilled.discharge_date,
    discharged_unbilled.summ_day_date,
    ROUND(discharged_unbilled.unbilled_charge_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_charge_amt,
    discharged_unbilled.unbilled_responsibility_ind,
    discharged_unbilled.account_process_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.discharged_unbilled
;
