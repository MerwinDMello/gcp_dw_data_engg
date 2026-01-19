-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.net_revenue_impact_denial
   OPTIONS(description='Daily Denial data to capture net revenue impact.')
  AS SELECT
      net_revenue_impact_denial.reporting_date,
      ROUND(net_revenue_impact_denial.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      net_revenue_impact_denial.iplan_id,
      net_revenue_impact_denial.iplan_insurance_order_num,
      ROUND(net_revenue_impact_denial.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      net_revenue_impact_denial.company_code,
      net_revenue_impact_denial.coid,
      net_revenue_impact_denial.unit_num,
      ROUND(net_revenue_impact_denial.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(net_revenue_impact_denial.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_denial.denial_status_code,
      net_revenue_impact_denial.patient_type_code,
      ROUND(net_revenue_impact_denial.write_off_denial_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_amt,
      ROUND(net_revenue_impact_denial.overturned_denial_amt, 3, 'ROUND_HALF_EVEN') AS overturned_denial_amt,
      net_revenue_impact_denial.dw_last_update_date_time,
      net_revenue_impact_denial.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.net_revenue_impact_denial
  ;
