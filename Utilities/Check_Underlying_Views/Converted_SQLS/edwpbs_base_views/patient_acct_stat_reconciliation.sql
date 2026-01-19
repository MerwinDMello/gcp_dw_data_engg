-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_acct_stat_reconciliation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_acct_stat_reconciliation
   OPTIONS(description='Daily snapshot of reconciliation of patient level statistical information, inpatient activity, outpatient activity etc  in Daily Management report')
  AS SELECT
      ROUND(patient_acct_stat_reconciliation.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      patient_acct_stat_reconciliation.reporting_date,
      ROUND(patient_acct_stat_reconciliation.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      patient_acct_stat_reconciliation.service_code,
      patient_acct_stat_reconciliation.crnt_patient_type_code,
      patient_acct_stat_reconciliation.prev_patient_type_code,
      patient_acct_stat_reconciliation.nursery_ind,
      patient_acct_stat_reconciliation.coid,
      patient_acct_stat_reconciliation.company_code,
      patient_acct_stat_reconciliation.sub_unit_num,
      ROUND(patient_acct_stat_reconciliation.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      patient_acct_stat_reconciliation.patient_full_name,
      patient_acct_stat_reconciliation.admission_date,
      patient_acct_stat_reconciliation.admission_day_cnt,
      patient_acct_stat_reconciliation.discharge_day_cnt,
      patient_acct_stat_reconciliation.patient_day_cnt,
      patient_acct_stat_reconciliation.charged_leave_of_abs_cnt,
      patient_acct_stat_reconciliation.non_charged_leave_of_abs_cnt,
      patient_acct_stat_reconciliation.pre_admit_cnt,
      patient_acct_stat_reconciliation.pre_admit_admission_cnt,
      patient_acct_stat_reconciliation.crnt_month_ind,
      patient_acct_stat_reconciliation.reg_cnt,
      patient_acct_stat_reconciliation.op_visit_cnt,
      patient_acct_stat_reconciliation.emrg_dept_visit_cnt,
      patient_acct_stat_reconciliation.surg_day_care_visit_cnt,
      patient_acct_stat_reconciliation.obsv_visit_cnt,
      patient_acct_stat_reconciliation.emrg_obsv_visit_cnt,
      patient_acct_stat_reconciliation.surg_day_care_obsv_visit_cnt,
      patient_acct_stat_reconciliation.obsv_day_cnt,
      patient_acct_stat_reconciliation.ip_admission_cnt,
      patient_acct_stat_reconciliation.op_admission_cnt,
      patient_acct_stat_reconciliation.pre_reg_op_cnt,
      patient_acct_stat_reconciliation.reg_of_pre_reg_op_cnt,
      patient_acct_stat_reconciliation.source_system_code,
      patient_acct_stat_reconciliation.dw_last_update_date_time,
      patient_acct_stat_reconciliation.column_36
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.patient_acct_stat_reconciliation
  ;
