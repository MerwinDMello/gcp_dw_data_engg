-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/pc_j_pf_fact_disc_rsld_pm_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(trim(format('%20d', coalesce(a.rowcount, 0))), trim(regexp_replace(format('%#22.3f', CAST(ROUND(coalesce(a.eor_gross_reimbursement_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(regexp_replace(format('%#22.3f', CAST(ROUND(coalesce(a.eor_contractual_allowance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(regexp_replace(format('%#22.3f', CAST(ROUND(coalesce(a.eor_insurance_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(regexp_replace(format('%#22.3f', CAST(coalesce(a.sum1, 0) AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(regexp_replace(format('%#22.3f', CAST(ROUND(coalesce(a.var_gross_reimbursement_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(regexp_replace(format('%#22.3f', CAST(ROUND(coalesce(a.var_contractual_allowance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(regexp_replace(format('%#22.3f', CAST(ROUND(coalesce(a.var_insurance_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(regexp_replace(format('%#22.3f', CAST(ROUND(coalesce(a.var_net_billed_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3'))) AS source_string
FROM
  (SELECT count(rsvd.patient_dw_id) AS rowcount,
          sum(eor.eor_gross_reimbursement_amt) AS eor_gross_reimbursement_amt,
          sum(eor.eor_contractual_allowance_amt) AS eor_contractual_allowance_amt,
          sum(eor.eor_insurance_payment_amt) AS eor_insurance_payment_amt,
          sum(0) AS sum1,
          sum(abs(rsvd.var_gross_reimbursement_amt)) AS var_gross_reimbursement_amt,
          sum(abs(rsvd.var_contractual_allowance_amt)) AS var_contractual_allowance_amt,
          sum(abs(rsvd.var_insurance_payment_amt)) AS var_insurance_payment_amt,
          sum(abs(rsvd.var_net_billed_charge_amt)) AS var_net_billed_charge_amt
   FROM {{ params.param_auth_base_views_dataset_name }}.resolved_discrepancy AS rsvd
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS eor ON upper(rsvd.company_code) = upper(eor.company_code)
   AND upper(rsvd.coid) = upper(eor.coid)
   AND rsvd.patient_dw_id = eor.patient_dw_id
   AND rsvd.payor_dw_id = eor.payor_dw_id
   AND rsvd.iplan_insurance_order_num = eor.iplan_insurance_order_num
   AND rsvd.eor_log_date = eor.eor_log_date
   AND upper(rsvd.log_id) = upper(eor.log_id)
   AND rsvd.log_sequence_num = eor.log_sequence_num
   AND coalesce(rsvd.cc_calc_id, CAST(999 AS BIGNUMERIC)) = coalesce(eor.cc_calc_id, CAST(999 AS BIGNUMERIC))
   LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_pars_discrepancy AS disc
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON disc.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                                                                                              WHEN '' THEN 0.0
                                                                                              ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS FLOAT64)
                                                                                          END
   AND upper(disc.company_code) = upper(rsvd.company_code)
   AND upper(disc.coid) = upper(rsvd.coid)
   AND disc.patient_dw_id = rsvd.patient_dw_id
   AND disc.payor_dw_id = rsvd.payor_dw_id
   AND disc.iplan_insurance_order_num = rsvd.iplan_insurance_order_num
   AND disc.eor_log_date = rsvd.eor_log_date
   AND disc.log_sequence_num = rsvd.log_sequence_num
   AND disc.ar_bill_thru_date = eor.ar_bill_thru_date
   AND disc.discrepancy_creation_date = rsvd.discrepancy_origination_date
   AND upper(disc.log_id) = upper(rsvd.log_id)
   AND disc.discrepancy_resolved_date = DATE '0001-01-01'
   LEFT OUTER JOIN
     (SELECT pt.patient_dw_id,
             max(pt.eff_from_date) AS eff_from_date
      FROM {{ params.param_auth_base_views_dataset_name }}.admission_patient_type AS pt
      GROUP BY 1) AS atp ON atp.patient_dw_id = rsvd.patient_dw_id
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.admission_patient_type AS apt ON apt.patient_dw_id = rsvd.patient_dw_id
   AND apt.eff_from_date = atp.eff_from_date
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.admission_discharge AS ad ON ad.patient_dw_id = rsvd.patient_dw_id
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility AS ff ON upper(ff.coid) = upper(rsvd.coid)
   AND upper(ff.company_code) = upper(rsvd.company_code)
   WHERE upper(format_date('%Y%m', rsvd.discrepancy_resolved_date)) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
     AND upper(format_date('%Y%m', rsvd.discrepancy_origination_date)) < upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
     AND eor.eff_from_date =
       (SELECT max(er.eff_from_date)
        FROM {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS er
        WHERE upper(rsvd.company_code) = upper(er.company_code)
          AND upper(rsvd.coid) = upper(er.coid)
          AND rsvd.patient_dw_id = er.patient_dw_id
          AND rsvd.payor_dw_id = er.payor_dw_id
          AND rsvd.iplan_insurance_order_num = er.iplan_insurance_order_num
          AND rsvd.eor_log_date = er.eor_log_date
          AND upper(rsvd.log_id) = upper(er.log_id)
          AND rsvd.log_sequence_num = er.log_sequence_num
          AND coalesce(rsvd.cc_calc_id, CAST(999 AS BIGNUMERIC)) = coalesce(er.cc_calc_id, CAST(999 AS BIGNUMERIC))
          AND er.eff_from_date <= rsvd.discrepancy_resolved_date )
     AND upper(rsvd.coid) NOT IN
       (SELECT upper(parallon_client_detail.coid) AS coid
        FROM {{ params.param_pbs_core_dataset_name }}.parallon_client_detail
        FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')
        WHERE upper(parallon_client_detail.company_code) = 'CHP' ) ) AS a