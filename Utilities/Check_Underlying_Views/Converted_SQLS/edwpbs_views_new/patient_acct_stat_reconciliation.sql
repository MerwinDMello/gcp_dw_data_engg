-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_acct_stat_reconciliation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_acct_stat_reconciliation AS SELECT
    a.patient_dw_id,
    a.reporting_date,
    a.financial_class_code,
    a.service_code,
    a.crnt_patient_type_code,
    a.prev_patient_type_code,
    a.nursery_ind,
    a.coid,
    a.effective_date,
    a.company_code,
    a.sub_unit_num,
    a.pat_acct_num,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_full_name
    END AS patient_full_name,
    a.admission_day_cnt,
    a.discharge_day_cnt,
    a.patient_day_cnt,
    a.charged_loa_cnt,
    a.non_charged_loa_cnt,
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
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_acct_stat_reconciliation AS a
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
