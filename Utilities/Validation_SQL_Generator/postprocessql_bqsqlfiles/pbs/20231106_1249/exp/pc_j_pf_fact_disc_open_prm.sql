-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/pc_j_pf_fact_disc_open_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat('0', trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(CAST(coalesce(sum(abs(rd.var_gross_reimbursement_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rd.var_contractual_allowance_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rd.var_insurance_payment_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rd.var_net_billed_charge_amt)), CAST(0 AS BIGNUMERIC)) AS STRING))) AS source_string
FROM `hca-hin-dev-cur-parallon`.auth_base_views.reimbursement_discrepancy_eom AS rd
LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.explanation_of_reimbursement AS eor ON upper(rd.company_code) = upper(eor.company_code)
AND upper(rd.coid) = upper(eor.coid)
AND rd.patient_dw_id = eor.patient_dw_id
AND rd.payor_dw_id = eor.payor_dw_id
AND rd.iplan_insurance_order_num = eor.iplan_insurance_order_num
AND rd.eor_log_date = eor.eor_log_date
AND upper(rd.log_id) = upper(eor.log_id)
AND rd.log_sequence_num = eor.log_sequence_num
AND coalesce(rd.cc_calc_id, CAST(999 AS BIGNUMERIC)) = coalesce(eor.cc_calc_id, CAST(999 AS BIGNUMERIC))
WHERE eor.eff_from_date =
    (SELECT max(er.eff_from_date)
     FROM `hca-hin-dev-cur-parallon`.auth_base_views.explanation_of_reimbursement AS er
     WHERE upper(rd.company_code) = upper(er.company_code)
       AND upper(rd.coid) = upper(er.coid)
       AND rd.patient_dw_id = er.patient_dw_id
       AND rd.payor_dw_id = er.payor_dw_id
       AND rd.iplan_insurance_order_num = er.iplan_insurance_order_num
       AND rd.eor_log_date = er.eor_log_date
       AND upper(rd.log_id) = upper(er.log_id)
       AND rd.log_sequence_num = er.log_sequence_num
       AND coalesce(rd.cc_calc_id, CAST(999 AS BIGNUMERIC)) = coalesce(er.cc_calc_id, CAST(999 AS BIGNUMERIC))
       AND er.eff_from_date <= date_sub(CAST(trim(concat(format_date('%Y-%m', current_date('US/Central')), '-01')) AS DATE), interval 1 DAY) )
  AND upper(rd.coid) NOT IN
    (SELECT upper(parallon_client_detail.coid) AS coid
     FROM `hca-hin-dev-cur-parallon`.edwpbs.parallon_client_detail
     WHERE upper(parallon_client_detail.company_code) = 'CHP' ) 