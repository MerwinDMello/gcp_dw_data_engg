CREATE OR REPLACE VIEW {{ params.param_rmt_views_dataset_name }}.fact_svc_line
AS SELECT
		a.svc_line_seq_num,
		a.patient_remit_sid,
		a.svc_line_cd,
		a.service_line_dt,
		a.service_end_dt,
		a.serv_line_charge_amt,
		a.serv_line_payment_amt,
		a.rev_code,
		a.unit_of_svc_paid_cnt,
		a.submitted_service_cd,
		a.dw_last_update_date_time,
		a.source_system_code,
		a.svc_line_cd_qual,
		a.proc_mod_1,
		a.proc_mod_2,
		a.proc_mod_3,
		a.proc_mod_4,
		a.customer_cd
  FROM
    {{ params.param_rmt_base_views_dataset_name }}.fact_svc_line AS a
  WHERE upper(rtrim(a.customer_cd, ' ')) IN(
    'HCA', 'HCAD'
  )
;
