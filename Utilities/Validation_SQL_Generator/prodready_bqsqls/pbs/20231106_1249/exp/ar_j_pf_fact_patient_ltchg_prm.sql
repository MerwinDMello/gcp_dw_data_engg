-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ar_j_pf_fact_patient_ltchg_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery
 -- locking table EDWPF.Patient_Suspended_Charge for access
-- locking table Edwfs.Fact_Facility for access
-- locking table edwpf.fact_rcom_ar_patient_level for access
-- locking table EDWPF.Patient_Suspended_Charge for access
-- locking table Edwfs.Fact_Facility for access
-- locking table edwpf.fact_rcom_ar_patient_level for access

SELECT concat(exp1.late_charge_credit_amt, exp1.late_charge_debit_amt) AS source_string
FROM
  (SELECT date_sub(date_add(CAST(trim(concat(format_date('%Y-%m', psc.charge_posted_date), '-01')) AS DATE), interval 1 MONTH), interval 6 DAY) AS date1,
          ERROR_UNIMPLEMENTED(/* No implementation for ExplicitCastNumericAsVarcharOpcode in BigQuery. */ sum(CASE
                                                                                                                  WHEN psc.charge_amt < 0 THEN psc.charge_amt
                                                                                                                  ELSE NUMERIC '0.00'
                                                                                                              END)) AS late_charge_credit_amt,
          ERROR_UNIMPLEMENTED(/* No implementation for ExplicitCastNumericAsVarcharOpcode in BigQuery. */ sum(CASE
                                                                                                                  WHEN psc.charge_amt >= 0 THEN psc.charge_amt
                                                                                                                  ELSE NUMERIC '0.00'
                                                                                                              END)) AS late_charge_debit_amt
   FROM {{ params.param_auth_base_views_dataset_name }}.patient_suspended_charge AS psc
   INNER JOIN -- - EDWPF.Patient_Suspended_Charge PSC old expected query
 -- -Join Edwfs.Fact_Facility FF - old expected query modified by Mahesh
 {{ params.param_auth_base_views_dataset_name }}.fact_facility AS ff ON upper(ff.coid) = upper(psc.coid)
   AND upper(ff.company_code) = upper(psc.company_code)
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_rcom_ar_patient_level AS fp ON fp.patient_sid = psc.pat_acct_num
   AND upper(fp.company_code) = upper(psc.company_code)
   AND upper(fp.coid) = upper(psc.coid)
   AND CASE format_date('%Y%m', psc.charge_posted_date)
           WHEN '' THEN 0
           ELSE CAST(format_date('%Y%m', psc.charge_posted_date) AS INT64)
       END = fp.date_sid
   AND fp.iplan_insurance_order_num = 0
   WHERE upper(format_date('%Y%m', psc.charge_posted_date)) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
     AND upper(psc.source_system_code) = 'P'
     AND (upper(psc.company_code) = 'H'
          OR upper(substr(trim(ff.company_code_operations), 1, 1)) = 'Y')
   GROUP BY 1) AS exp1