-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/discharged_unbilled.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.discharged_unbilled AS SELECT
    discharged_unbilled.coid,
    discharged_unbilled.company_code,
    discharged_unbilled.patient_dw_id,
    discharged_unbilled.effective_date,
    discharged_unbilled.unit_num,
    discharged_unbilled.pat_acct_num,
    discharged_unbilled.discharge_date,
    discharged_unbilled.summ_day_date,
    discharged_unbilled.unbilled_charge_amt,
    discharged_unbilled.unbilled_responsibility_ind,
    discharged_unbilled.account_process_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.discharged_unbilled
;
