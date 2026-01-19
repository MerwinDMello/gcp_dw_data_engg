-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_acct_stat_reconciliation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_acct_stat_reconciliation AS SELECT
    patient_acct_stat_reconciliation.patient_dw_id,
    patient_acct_stat_reconciliation.reporting_date,
    patient_acct_stat_reconciliation.financial_class_code,
    patient_acct_stat_reconciliation.service_code,
    patient_acct_stat_reconciliation.crnt_patient_type_code,
    patient_acct_stat_reconciliation.prev_patient_type_code,
    patient_acct_stat_reconciliation.nursery_ind,
    patient_acct_stat_reconciliation.coid,
    patient_acct_stat_reconciliation.effective_date,
    patient_acct_stat_reconciliation.company_code,
    patient_acct_stat_reconciliation.sub_unit_num,
    patient_acct_stat_reconciliation.pat_acct_num,
    patient_acct_stat_reconciliation.patient_full_name,
    patient_acct_stat_reconciliation.admission_day_cnt,
    patient_acct_stat_reconciliation.discharge_day_cnt,
    patient_acct_stat_reconciliation.patient_day_cnt,
    patient_acct_stat_reconciliation.charged_loa_cnt,
    patient_acct_stat_reconciliation.non_charged_loa_cnt,
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
    patient_acct_stat_reconciliation.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.patient_acct_stat_reconciliation
;
