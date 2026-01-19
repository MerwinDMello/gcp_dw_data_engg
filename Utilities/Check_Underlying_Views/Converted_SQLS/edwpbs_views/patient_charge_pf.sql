-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_charge_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_charge_pf AS SELECT
    pc.coid,
    pc.company_code,
    ROUND(pc.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    pc.charge_code,
    pc.service_date,
    ROUND(pc.charge_posted_date_time, 0, 'ROUND_HALF_EVEN') AS charge_posted_date_time,
    pc.origin_id,
    pc.room_bed_num,
    pc.revenue_code,
    pc.credit_ind,
    ROUND(pc.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    pc.gl_dept_num,
    pc.gl_sub_account_num,
    pc.mnemonic_code,
    ROUND(pc.charge_amt, 3, 'ROUND_HALF_EVEN') AS charge_amt,
    pc.charge_desc,
    pc.manual_change_ind,
    pc.factor_qty,
    pc.professional_component_ind,
    ROUND(pc.batch_day_num, 0, 'ROUND_HALF_EVEN') AS batch_day_num,
    ROUND(pc.batch_month_num, 0, 'ROUND_HALF_EVEN') AS batch_month_num,
    pc.patient_charge_type_code,
    pc.facility_physician_intr_num,
    pc.facility_physician_ord_num,
    pc.charge_posted_date,
    pc.charge_posted_date_time_ts,
    pc.pe_date,
    pc.dept_num,
    pc.non_covered_charge_flag,
    pc.fee_schedule_code,
    pc.place_of_service_code,
    pc.non_bill_reason_code,
    pc.non_bill_gl_acct_num,
    pc.non_bill_manual_update_ind,
    pc.bill_through_date,
    pc.partb_charge_ind_ins1,
    pc.partb_charge_ind_ins2,
    pc.partb_charge_ind_ins3,
    pc.original_service_date,
    pc.distributed_system_code,
    pc.source_system_code,
    pc.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_charge_pf AS pc
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON upper(pc.coid) = upper(sf.co_id)
     AND sf.user_id = session_user()
;
