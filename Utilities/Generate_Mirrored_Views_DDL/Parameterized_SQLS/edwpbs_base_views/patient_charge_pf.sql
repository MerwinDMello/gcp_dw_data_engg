-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_charge_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.patient_charge_pf AS SELECT
    patient_charge.coid,
    patient_charge.company_code,
    ROUND(patient_charge.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    patient_charge.charge_code,
    patient_charge.service_date,
    ROUND(patient_charge.charge_posted_date_time, 0, 'ROUND_HALF_EVEN') AS charge_posted_date_time,
    patient_charge.origin_id,
    patient_charge.room_bed_num,
    patient_charge.revenue_code,
    patient_charge.credit_ind,
    ROUND(patient_charge.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    patient_charge.gl_dept_num,
    patient_charge.gl_sub_account_num,
    patient_charge.mnemonic_code,
    ROUND(patient_charge.charge_amt, 3, 'ROUND_HALF_EVEN') AS charge_amt,
    patient_charge.charge_desc,
    patient_charge.manual_change_ind,
    patient_charge.factor_qty,
    patient_charge.professional_component_ind,
    ROUND(patient_charge.batch_day_num, 0, 'ROUND_HALF_EVEN') AS batch_day_num,
    ROUND(patient_charge.batch_month_num, 0, 'ROUND_HALF_EVEN') AS batch_month_num,
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
    {{ params.param_auth_base_views_dataset_name }}.patient_charge
;
