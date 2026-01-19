-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_prorated_liability.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_prorated_liability AS SELECT
    payor_prorated_liability.patient_dw_id,
    payor_prorated_liability.payor_dw_id,
    payor_prorated_liability.iplan_insurance_order_num,
    payor_prorated_liability.person_role_code,
    payor_prorated_liability.iplan_id,
    payor_prorated_liability.pat_acct_num,
    payor_prorated_liability.eff_to_date,
    payor_prorated_liability.prorated_liability_amt,
    payor_prorated_liability.prorated_released_amt,
    payor_prorated_liability.prorated_released_date,
    payor_prorated_liability.source_system_code,
    payor_prorated_liability.eff_from_date,
    payor_prorated_liability.claim_submit_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.payor_prorated_liability
;
