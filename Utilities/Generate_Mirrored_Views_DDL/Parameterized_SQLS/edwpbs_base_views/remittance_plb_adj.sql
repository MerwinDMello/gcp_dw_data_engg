-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_plb_adj.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.remittance_plb_adj
   OPTIONS(description='This table contains information related to Provider level Balance adjustments associated with the payment checks.')
  AS SELECT
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
      ROUND(remittance_plb_adj.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
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
      {{ params.param_pbs_core_dataset_name }}.remittance_plb_adj
  ;
