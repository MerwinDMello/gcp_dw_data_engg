-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_charge_mv_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_charge_mv_pf AS SELECT
    patient_charge_mv.coid,
    patient_charge_mv.company_code,
    patient_charge_mv.patient_dw_id,
    patient_charge_mv.charge_code,
    patient_charge_mv.service_date,
    patient_charge_mv.charge_posted_date_time,
    patient_charge_mv.origin_id,
    patient_charge_mv.room_bed_num,
    patient_charge_mv.revenue_code,
    patient_charge_mv.credit_ind,
    patient_charge_mv.pat_acct_num,
    patient_charge_mv.gl_dept_num,
    patient_charge_mv.gl_sub_account_num,
    patient_charge_mv.mnemonic_code,
    patient_charge_mv.charge_amt,
    patient_charge_mv.charge_desc,
    patient_charge_mv.manual_change_ind,
    patient_charge_mv.factor_qty,
    patient_charge_mv.professional_component_ind,
    patient_charge_mv.batch_day_num,
    patient_charge_mv.batch_month_num,
    patient_charge_mv.patient_charge_type_code,
    patient_charge_mv.facility_physician_intr_num,
    patient_charge_mv.facility_physician_ord_num,
    patient_charge_mv.charge_posted_date,
    patient_charge_mv.charge_posted_date_time_ts,
    patient_charge_mv.pe_date,
    patient_charge_mv.dept_num,
    patient_charge_mv.non_covered_charge_flag,
    patient_charge_mv.fee_schedule_code,
    patient_charge_mv.place_of_service_code,
    patient_charge_mv.non_bill_reason_code,
    patient_charge_mv.non_bill_gl_acct_num,
    patient_charge_mv.non_bill_manual_update_ind,
    patient_charge_mv.bill_through_date,
    patient_charge_mv.partb_charge_ind_ins1,
    patient_charge_mv.partb_charge_ind_ins2,
    patient_charge_mv.partb_charge_ind_ins3,
    patient_charge_mv.original_service_date,
    patient_charge_mv.distributed_system_code,
    patient_charge_mv.source_system_code,
    patient_charge_mv.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.patient_charge_mv
;
