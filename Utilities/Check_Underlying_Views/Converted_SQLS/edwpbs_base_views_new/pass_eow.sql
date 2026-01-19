-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/pass_eow.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.pass_eow AS SELECT
    pass_eow.company_code,
    pass_eow.coid,
    pass_eow.reporting_date,
    pass_eow.patient_dw_id,
    pass_eow.log_id,
    pass_eow.iplan_id,
    pass_eow.patient_type_code,
    pass_eow.financial_class_code,
    pass_eow.unit_num,
    pass_eow.payor_dw_id,
    pass_eow.pat_acct_num,
    pass_eow.final_bill_date,
    pass_eow.account_status_code,
    pass_eow.total_billed_charge_amount,
    pass_eow.eor_gross_reimbursement_amount,
    pass_eow.total_account_balance_amount
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.pass_eow
;
