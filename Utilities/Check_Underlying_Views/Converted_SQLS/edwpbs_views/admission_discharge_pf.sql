-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/admission_discharge_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.admission_discharge_pf AS SELECT
    a.coid,
    a.company_code,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.discharge_medical_code,
    a.discharge_status_code,
    a.discharge_date,
    a.discharge_hour,
    a.discharge_posted_date,
    a.source_system_code,
    a.final_bill_date,
    a.final_bill_hold_ind,
    a.hold_bill_reason_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.admission_discharge_pf AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
     AND upper(a.coid) = upper(b.co_id)
     AND b.user_id = session_user()
;
