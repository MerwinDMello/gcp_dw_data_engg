CREATE OR REPLACE VIEW {{ params.param_rmt_mirrored_base_views_dataset_name }}.fact_svc_line
AS SELECT
		fact_svc_line.svc_line_seq_num,
		fact_svc_line.patient_remit_sid,
		fact_svc_line.svc_line_cd,
		fact_svc_line.service_line_dt,
		fact_svc_line.service_end_dt,
		fact_svc_line.serv_line_charge_amt,
		fact_svc_line.serv_line_payment_amt,
		fact_svc_line.rev_code,
		fact_svc_line.unit_of_svc_paid_cnt,
		fact_svc_line.submitted_service_cd,
		fact_svc_line.dw_last_update_date_time,
		fact_svc_line.source_system_code,
		fact_svc_line.svc_line_cd_qual,
		fact_svc_line.proc_mod_1,
		fact_svc_line.proc_mod_2,
		fact_svc_line.proc_mod_3,
		fact_svc_line.proc_mod_4,
		fact_svc_line.customer_cd
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_rmt_mirrored_core_dataset_name }}.fact_svc_line
;
