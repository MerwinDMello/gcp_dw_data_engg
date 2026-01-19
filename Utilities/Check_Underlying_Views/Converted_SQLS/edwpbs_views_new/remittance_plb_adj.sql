-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_plb_adj.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.remittance_plb_adj AS SELECT
    a.payment_guid,
    a.audit_date,
    a.adj_reason_code,
    a.adj_ref_id,
    a.adj_record_num,
    a.delete_ind,
    a.delete_date,
    a.unit_num,
    a.iplan_id,
    a.procedure_code,
    a.adj_amt,
    a.adj_match_code,
    a.payer_claim_control_number,
    a.ep_calc_claim_control_num,
    a.discharge_date,
    a.fiscal_period_date,
    a.patient_adj_ind,
    a.general_ledger_adj_ind,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_plb_adj AS a
;
