-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_charge_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_charge_pf AS SELECT
    patient_charge.coid,
    patient_charge.company_code,
    patient_charge.patient_dw_id,
    patient_charge.charge_code,
    patient_charge.service_date,
    patient_charge.charge_posted_date_time,
    patient_charge.origin_id,
    patient_charge.room_bed_num,
    patient_charge.revenue_code,
    patient_charge.credit_ind,
    patient_charge.pat_acct_num,
    patient_charge.gl_dept_num,
    patient_charge.gl_sub_account_num,
    patient_charge.mnemonic_code,
    patient_charge.charge_amt,
    patient_charge.charge_desc,
    patient_charge.manual_change_ind,
    patient_charge.factor_qty,
    patient_charge.professional_component_ind,
    patient_charge.batch_day_num,
    patient_charge.batch_month_num,
    patient_charge.patient_charge_type_code,
    patient_charge.facility_physician_intr_num,
    patient_charge.facility_physician_ord_num,
    patient_charge.charge_posted_date,
    patient_charge.charge_posted_date_time_ts,
    patient_charge.pe_date,
    patient_charge.dept_num,
    patient_charge.non_covered_charge_flag,
    patient_charge.fee_schedule_code,
    patient_charge.place_of_service_code,
    patient_charge.non_bill_reason_code,
    patient_charge.non_bill_gl_acct_num,
    patient_charge.non_bill_manual_update_ind,
    patient_charge.bill_through_date,
    patient_charge.partb_charge_ind_ins1,
    patient_charge.partb_charge_ind_ins2,
    patient_charge.partb_charge_ind_ins3,
    patient_charge.original_service_date,
    patient_charge.distributed_system_code,
    patient_charge.source_system_code,
    patient_charge.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.patient_charge
;
