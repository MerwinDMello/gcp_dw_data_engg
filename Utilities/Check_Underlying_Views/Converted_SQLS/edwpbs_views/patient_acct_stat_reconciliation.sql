-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_acct_stat_reconciliation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_acct_stat_reconciliation
   OPTIONS(description='Daily snapshot of reconciliation of patient level statistical information, inpatient activity, outpatient activity etc  in Daily Management report')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.reporting_date,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.service_code,
      a.crnt_patient_type_code,
      a.prev_patient_type_code,
      a.nursery_ind,
      a.coid,
      a.company_code,
      a.sub_unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      CASE
        WHEN session_user() = pn.userid THEN '***'
        ELSE a.patient_full_name
      END AS patient_full_name,
      a.admission_date,
      a.admission_day_cnt,
      a.discharge_day_cnt,
      a.patient_day_cnt,
      a.charged_leave_of_abs_cnt,
      a.non_charged_leave_of_abs_cnt,
      a.pre_admit_cnt,
      a.pre_admit_admission_cnt,
      a.crnt_month_ind,
      a.reg_cnt,
      a.op_visit_cnt,
      a.emrg_dept_visit_cnt,
      a.surg_day_care_visit_cnt,
      a.obsv_visit_cnt,
      a.emrg_obsv_visit_cnt,
      a.surg_day_care_obsv_visit_cnt,
      a.obsv_day_cnt,
      a.ip_admission_cnt,
      a.op_admission_cnt,
      a.pre_reg_op_cnt,
      a.reg_of_pre_reg_op_cnt,
      a.source_system_code,
      a.dw_last_update_date_time,
      a.column_36
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_acct_stat_reconciliation AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'PN'
      ) AS pn ON pn.userid = session_user()
  ;
