-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/him_unbilled_daily.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.him_unbilled_daily AS SELECT
    a.unit_num,
    a.effective_date,
    a.pat_acct_num,
    a.company_code,
    a.coid,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_name
    END AS patient_name,
    a.pa_pat_acct_type,
    a.him_pat_acct_type,
    a.medical_record_num,
    a.admission_date,
    a.discharge_date,
    a.final_bill_date,
    a.days_since_discharge,
    a.iplan_id_ins1,
    a.financial_class_code,
    a.gross_charge_amt,
    a.total_account_balance_amt,
    a.account_process_ind,
    a.unbilled_responsibility_ind,
    a.him_responsibility_ind,
    a.him_location,
    a.service_code_mnemonic,
    a.patient_discharge_month_age,
    a.unbilled_reason_code,
    a.responsible_physician,
    a.responsible_physician_mnemonic,
    a.attending_physician,
    a.attending_physician_mnemonic
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.him_unbilled_daily AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND security_mask_and_exception.masked_column_code = 'PN'
    ) AS pn ON pn.userid = session_user()
;
