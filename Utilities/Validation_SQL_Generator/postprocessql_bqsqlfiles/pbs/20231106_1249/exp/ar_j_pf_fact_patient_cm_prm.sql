-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ar_j_pf_fact_patient_cm_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT trim(format('%20d', coalesce(count(*), 0))) AS source_string
FROM
  (SELECT fp.patient_sid,
          fp.account_type_sid,
          fp.account_status_sid,
          fp.age_month_sid,
          fp.patient_financial_class_sid,
          fp.patient_type_sid,
          fp.collection_agency_sid,
          fp.payor_financial_class_sid,
          fp.product_sid,
          fp.contract_sid,
          fp.scenario_sid,
          fp.date_sid,
          fp.source_sid,
          fp.unit_num_sid,
          fp.iplan_insurance_order_num,
          fp.coid,
          fp.company_code,
          fp.patient_account_cnt,
          fp.discharge_cnt,
          fp.ar_patient_amt,
          fp.ar_insurance_amt,
          fp.write_off_amt,
          fp.total_collect_amt,
          fp.billed_patient_cnt,
          fp.discharge_to_billing_day_cnt,
          fp.gross_charge_amt,
          fp.prorated_liability_sys_adj_amt,
          fp.late_charge_credit_amt,
          fp.late_charge_debit_amt,
          fp.payor_prorated_liability_amt,
          fp.payor_payment_amt,
          fp.payor_adjustment_amt,
          fp.payor_contractual_amt,
          fp.payor_denial_amt,
          fp.payor_denial_cnt,
          fp.payor_expected_payment_amt,
          fp.payor_discrepancy_ovr_pmt_amt,
          fp.payor_discrepancy_undr_pmt_amt,
          fp.payor_up_front_collection_amt,
          fp.payor_bill_cnt,
          fp.payor_rebill_cnt,
          fp.payor_sid,
          fp.unbilled_gross_bus_ofc_amt,
          fp.unbilled_gross_med_rec_amt
   FROM `hca-hin-dev-cur-parallon`.auth_base_views.fact_rcom_ar_patient_level AS fp
   WHERE fp.date_sid =
       (SELECT max(eis_date_dim.time_id)
        FROM `hca-hin-dev-cur-parallon`.auth_base_views.eis_date_dim
        WHERE upper(eis_date_dim.current_mth) = 'Y' ) ) AS p 