-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_charge_crnt.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_charge_crnt AS SELECT
    a.coid,
    a.company_code,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    a.charge_code,
    a.service_date,
    ROUND(a.charge_posted_date_time, 0, 'ROUND_HALF_EVEN') AS charge_posted_date_time,
    a.origin_id,
    a.room_bed_num,
    a.revenue_code,
    a.credit_ind,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.gl_dept_num,
    a.gl_sub_account_num,
    a.mnemonic_code,
    ROUND(a.charge_amt, 3, 'ROUND_HALF_EVEN') AS charge_amt,
    a.charge_desc,
    a.manual_change_ind,
    a.factor_qty,
    a.professional_component_ind,
    ROUND(a.batch_day_num, 0, 'ROUND_HALF_EVEN') AS batch_day_num,
    ROUND(a.batch_month_num, 0, 'ROUND_HALF_EVEN') AS batch_month_num,
    a.patient_charge_type_code,
    a.facility_physician_intr_num,
    a.facility_physician_ord_num,
    a.charge_posted_date,
    a.charge_posted_date_time_ts,
    a.pe_date,
    a.dept_num,
    a.non_covered_charge_flag,
    a.fee_schedule_code,
    a.place_of_service_code,
    a.non_bill_reason_code,
    a.non_bill_gl_acct_num,
    a.non_bill_manual_update_ind,
    a.bill_through_date,
    a.partb_charge_ind_ins1,
    a.partb_charge_ind_ins2,
    a.partb_charge_ind_ins3,
    a.original_service_date,
    a.distributed_system_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_charge_mv_pf AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
     AND upper(a.coid) = upper(b.co_id)
     AND b.user_id = session_user()
;
