-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_plb_adj.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.remittance_plb_adj
   OPTIONS(description='This table contains information related to Provider level Balance adjustments associated with the payment checks.')
  AS SELECT
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
      ROUND(a.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
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
      {{ params.param_pbs_base_views_dataset_name }}.remittance_plb_adj AS a
  ;
