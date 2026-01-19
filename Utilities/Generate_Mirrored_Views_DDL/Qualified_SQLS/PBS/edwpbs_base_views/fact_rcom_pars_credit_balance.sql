-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_credit_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_pars_credit_balance
   OPTIONS(description='This table contains Credit Balance related details of all patient accounts')
  AS SELECT
      ROUND(fact_rcom_pars_credit_balance.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      fact_rcom_pars_credit_balance.unit_num_sid,
      fact_rcom_pars_credit_balance.account_status_sid,
      fact_rcom_pars_credit_balance.patient_financial_class_sid,
      fact_rcom_pars_credit_balance.patient_type_sid,
      fact_rcom_pars_credit_balance.payor_sid,
      fact_rcom_pars_credit_balance.payor_sequence_sid,
      fact_rcom_pars_credit_balance.credit_status_sid,
      fact_rcom_pars_credit_balance.cr_bal_orig_age_month_sid,
      fact_rcom_pars_credit_balance.date_sid,
      ROUND(fact_rcom_pars_credit_balance.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_rcom_pars_credit_balance.company_code,
      fact_rcom_pars_credit_balance.coid,
      ROUND(fact_rcom_pars_credit_balance.credit_balance_amt, 3, 'ROUND_HALF_EVEN') AS credit_balance_amt,
      fact_rcom_pars_credit_balance.credit_balance_count,
      fact_rcom_pars_credit_balance.discharge_date,
      fact_rcom_pars_credit_balance.credit_balance_date,
      fact_rcom_pars_credit_balance.created_date,
      fact_rcom_pars_credit_balance.source_system_code,
      fact_rcom_pars_credit_balance.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_pars_credit_balance
  ;
