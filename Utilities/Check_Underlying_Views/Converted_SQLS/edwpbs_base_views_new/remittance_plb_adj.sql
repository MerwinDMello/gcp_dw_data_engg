-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_plb_adj.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_plb_adj AS SELECT
    remittance_plb_adj.payment_guid,
    remittance_plb_adj.audit_date,
    remittance_plb_adj.adj_reason_code,
    remittance_plb_adj.adj_ref_id,
    remittance_plb_adj.adj_record_num,
    remittance_plb_adj.delete_ind,
    remittance_plb_adj.delete_date,
    remittance_plb_adj.unit_num,
    remittance_plb_adj.iplan_id,
    remittance_plb_adj.procedure_code,
    remittance_plb_adj.adj_amt,
    remittance_plb_adj.adj_match_code,
    remittance_plb_adj.payer_claim_control_number,
    remittance_plb_adj.ep_calc_claim_control_num,
    remittance_plb_adj.discharge_date,
    remittance_plb_adj.fiscal_period_date,
    remittance_plb_adj.patient_adj_ind,
    remittance_plb_adj.general_ledger_adj_ind,
    remittance_plb_adj.source_system_code,
    remittance_plb_adj.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.remittance_plb_adj
;
