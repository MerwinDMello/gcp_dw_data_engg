-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/pass_eow.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.pass_eow AS SELECT
    a.company_code,
    a.coid,
    a.reporting_date,
    a.patient_dw_id,
    a.log_id,
    a.iplan_id,
    a.patient_type_code,
    a.financial_class_code,
    a.unit_num,
    a.payor_dw_id,
    a.pat_acct_num,
    a.final_bill_date,
    a.account_status_code,
    a.total_billed_charge_amount,
    a.eor_gross_reimbursement_amount,
    a.total_account_balance_amount
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.pass_eow AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
