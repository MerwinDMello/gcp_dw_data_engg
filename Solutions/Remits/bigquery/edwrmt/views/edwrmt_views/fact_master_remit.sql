CREATE OR REPLACE VIEW {{ params.param_rmt_views_dataset_name }}.fact_master_remit
AS SELECT
    a.remit_id,
    a.remit_entered_dt,
    a.remit_effective_dt,
    a.remit_total_amt,
    a.check_num,
    a.payment_type_ind,
    a.tran_handle_cd,
    a.credit_debit_ind,
    a.pay_method_cd,
    a.pay_fmt_cd,
    a.sender_remit_id_qual_ind,
    a.sender_remit_num,
    a.sender_acct_num,
    a.sender_acct_num_qual_ind,
    a.receiver_remit_id_qual_ind,
    a.receiver_remit_num,
    a.receiver_acct_num_qual_ind,
    a.receiver_acct_num,
    a.dw_last_update_date_time,
    a.source_system_code,
    a.customer_cd,
    a.sender_identifier,
    a.receiver_identifier,
    a.interchange_control_num,
    a.payer_identifier
  FROM
    {{ params.param_rmt_base_views_dataset_name }}.fact_master_remit AS a
  WHERE upper(rtrim(a.customer_cd, ' ')) IN(
    'HCA', 'HCAD'
  )
;
