-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_account_pymt_rptg_data.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_account_pymt_rptg_data
   OPTIONS(description='This table contains payment based flags and details to understand an accounts payor changes, Billing cycle etc.')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.iplan_insurance_order_num,
      a.coid,
      a.company_code,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.iplan_id,
      ROUND(a.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.major_payor_group_id,
      a.weekend_cnt,
      a.distinct_iplan_change_cnt,
      a.iplan_change_cnt,
      a.mpg_id_change_cnt,
      a.billed_claims_cnt,
      a.partial_denial_ind,
      a.potential_recoup_ind,
      a.first_iplan_id,
      ROUND(a.first_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS first_payor_financial_class_code,
      a.first_major_payor_group_id,
      a.last_iplan_id,
      ROUND(a.last_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS last_payor_financial_class_code,
      a.last_major_payor_group_id,
      a.adm_iplan_id,
      ROUND(a.adm_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS adm_payor_financial_class_code,
      a.adm_major_payor_group_id,
      a.dchg_iplan_id,
      ROUND(a.dchg_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS dchg_payor_financial_class_code,
      a.dchg_major_payor_group_id,
      a.final_billed_iplan_id,
      ROUND(a.final_billed_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS final_billed_payor_financial_class_code,
      a.final_billed_major_payor_group_id,
      a.first_claim_iplan_id,
      ROUND(a.first_claim_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS first_claim_payor_financial_class_code,
      a.first_claim_major_payor_group_id,
      a.last_claim_iplan_id,
      ROUND(a.last_claim_payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS last_claim_payor_financial_class_code,
      a.last_claim_major_payor_group_id,
      a.first_date_of_claim,
      a.first_bill_type_code,
      a.first_bill_rplc_pri_claim_ind,
      a.last_date_of_claim,
      a.last_bill_type_code,
      a.last_bill_rplc_pri_claim_ind,
      a.claim_bill_type_ip_ind,
      a.claim_bill_type_op_ind,
      a.bill_type_rplc_pri_claim_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_pymt_rptg_data AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
