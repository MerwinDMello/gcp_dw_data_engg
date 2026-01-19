-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/pc_j_pf_fact_disc_rsld_cm_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(trim(format('%20d', coalesce(count(rsvd.patient_dw_id), 0))), trim(CAST(coalesce(sum(eor.eor_gross_reimbursement_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(eor.eor_contractual_allowance_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(eor.eor_insurance_payment_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(format('%4d', coalesce(sum(0), 0))), trim(CAST(coalesce(sum(abs(rsvd.var_gross_reimbursement_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rsvd.var_contractual_allowance_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rsvd.var_insurance_payment_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rsvd.var_net_billed_charge_amt)), CAST(0 AS BIGNUMERIC)) AS STRING))) AS source_string
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
  AND upper(format_date('%Y%m', rsvd.discrepancy_origination_date)) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
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
     WHERE upper(parallon_client_detail.company_code) = 'CHP' )