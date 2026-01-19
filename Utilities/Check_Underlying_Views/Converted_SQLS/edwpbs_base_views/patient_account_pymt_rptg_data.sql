-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_account_pymt_rptg_data.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_pymt_rptg_data
   OPTIONS(description='This table contains payment based flags and details to understand an accounts payor changes, Billing cycle etc.')
  AS SELECT
      ROUND(patient_account_pymt_rptg_data.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      patient_account_pymt_rptg_data.iplan_insurance_order_num,
      patient_account_pymt_rptg_data.coid,
      patient_account_pymt_rptg_data.company_code,
      patient_account_pymt_rptg_data.unit_num,
      ROUND(patient_account_pymt_rptg_data.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      patient_account_pymt_rptg_data.iplan_id,
      ROUND(patient_account_pymt_rptg_data.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      ROUND(patient_account_pymt_rptg_data.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      patient_account_pymt_rptg_data.major_payor_group_id,
      patient_account_pymt_rptg_data.weekend_cnt,
      patient_account_pymt_rptg_data.distinct_iplan_change_cnt,
      patient_account_pymt_rptg_data.iplan_change_cnt,
      patient_account_pymt_rptg_data.mpg_id_change_cnt,
      patient_account_pymt_rptg_data.billed_claims_cnt,
      patient_account_pymt_rptg_data.partial_denial_ind,
      patient_account_pymt_rptg_data.potential_recoup_ind,
      patient_account_pymt_rptg_data.first_iplan_id,
      ROUND(patient_account_pymt_rptg_data.first_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS first_payor_financial_class_code,
      patient_account_pymt_rptg_data.first_major_payor_group_id,
      patient_account_pymt_rptg_data.last_iplan_id,
      ROUND(patient_account_pymt_rptg_data.last_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS last_payor_financial_class_code,
      patient_account_pymt_rptg_data.last_major_payor_group_id,
      patient_account_pymt_rptg_data.adm_iplan_id,
      ROUND(patient_account_pymt_rptg_data.adm_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS adm_payor_financial_class_code,
      patient_account_pymt_rptg_data.adm_major_payor_group_id,
      patient_account_pymt_rptg_data.dchg_iplan_id,
      ROUND(patient_account_pymt_rptg_data.dchg_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS dchg_payor_financial_class_code,
      patient_account_pymt_rptg_data.dchg_major_payor_group_id,
      patient_account_pymt_rptg_data.final_billed_iplan_id,
      ROUND(patient_account_pymt_rptg_data.final_billed_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS final_billed_payor_financial_class_code,
      patient_account_pymt_rptg_data.final_billed_major_payor_group_id,
      patient_account_pymt_rptg_data.first_claim_iplan_id,
      ROUND(patient_account_pymt_rptg_data.first_claim_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS first_claim_payor_financial_class_code,
      patient_account_pymt_rptg_data.first_claim_major_payor_group_id,
      patient_account_pymt_rptg_data.last_claim_iplan_id,
      ROUND(patient_account_pymt_rptg_data.last_claim_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS last_claim_payor_financial_class_code,
      patient_account_pymt_rptg_data.last_claim_major_payor_group_id,
      patient_account_pymt_rptg_data.first_date_of_claim,
      patient_account_pymt_rptg_data.first_bill_type_code,
      patient_account_pymt_rptg_data.first_bill_rplc_pri_claim_ind,
      patient_account_pymt_rptg_data.last_date_of_claim,
      patient_account_pymt_rptg_data.last_bill_type_code,
      patient_account_pymt_rptg_data.last_bill_rplc_pri_claim_ind,
      patient_account_pymt_rptg_data.claim_bill_type_ip_ind,
      patient_account_pymt_rptg_data.claim_bill_type_op_ind,
      patient_account_pymt_rptg_data.bill_type_rplc_pri_claim_ind,
      patient_account_pymt_rptg_data.source_system_code,
      patient_account_pymt_rptg_data.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.patient_account_pymt_rptg_data
  ;
