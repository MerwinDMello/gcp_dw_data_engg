-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_account_pymt_rptg_data.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_pymt_rptg_data AS SELECT
    patient_account_pymt_rptg_data.patient_dw_id,
    patient_account_pymt_rptg_data.iplan_insurance_order_num,
    patient_account_pymt_rptg_data.coid,
    patient_account_pymt_rptg_data.company_code,
    patient_account_pymt_rptg_data.unit_num,
    patient_account_pymt_rptg_data.pat_acct_num,
    patient_account_pymt_rptg_data.iplan_id,
    patient_account_pymt_rptg_data.payor_financial_class_code,
    patient_account_pymt_rptg_data.payor_dw_id,
    patient_account_pymt_rptg_data.major_payor_group_id,
    patient_account_pymt_rptg_data.weekend_cnt,
    patient_account_pymt_rptg_data.distinct_iplan_change_cnt,
    patient_account_pymt_rptg_data.iplan_change_cnt,
    patient_account_pymt_rptg_data.mpg_id_change_cnt,
    patient_account_pymt_rptg_data.billed_claims_cnt,
    patient_account_pymt_rptg_data.partial_denial_ind,
    patient_account_pymt_rptg_data.potential_recoup_ind,
    patient_account_pymt_rptg_data.first_iplan_id,
    patient_account_pymt_rptg_data.first_payor_financial_class_code,
    patient_account_pymt_rptg_data.first_major_payor_group_id,
    patient_account_pymt_rptg_data.last_iplan_id,
    patient_account_pymt_rptg_data.last_payor_financial_class_code,
    patient_account_pymt_rptg_data.last_major_payor_group_id,
    patient_account_pymt_rptg_data.adm_iplan_id,
    patient_account_pymt_rptg_data.adm_payor_financial_class_code,
    patient_account_pymt_rptg_data.adm_major_payor_group_id,
    patient_account_pymt_rptg_data.dchg_iplan_id,
    patient_account_pymt_rptg_data.dchg_payor_financial_class_code,
    patient_account_pymt_rptg_data.dchg_major_payor_group_id,
    patient_account_pymt_rptg_data.final_billed_iplan_id,
    patient_account_pymt_rptg_data.final_billed_payor_financial_class_code,
    patient_account_pymt_rptg_data.final_billed_major_payor_group_id,
    patient_account_pymt_rptg_data.first_claim_iplan_id,
    patient_account_pymt_rptg_data.first_claim_payor_financial_class_code,
    patient_account_pymt_rptg_data.first_claim_major_payor_group_id,
    patient_account_pymt_rptg_data.last_claim_iplan_id,
    patient_account_pymt_rptg_data.last_claim_payor_financial_class_code,
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
