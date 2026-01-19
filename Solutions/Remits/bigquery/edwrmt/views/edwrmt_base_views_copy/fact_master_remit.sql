CREATE OR REPLACE VIEW {{ params.param_rmt_mirrored_base_views_dataset_name }}.fact_master_remit
AS SELECT
    fact_master_remit.remit_id,
    fact_master_remit.remit_entered_dt,
    fact_master_remit.remit_effective_dt,
    fact_master_remit.remit_total_amt,
    fact_master_remit.check_num,
    fact_master_remit.payment_type_ind,
    fact_master_remit.tran_handle_cd,
    fact_master_remit.credit_debit_ind,
    fact_master_remit.pay_method_cd,
    fact_master_remit.pay_fmt_cd,
    fact_master_remit.sender_remit_id_qual_ind,
    fact_master_remit.sender_remit_num,
    fact_master_remit.sender_acct_num,
    fact_master_remit.sender_acct_num_qual_ind,
    fact_master_remit.receiver_remit_id_qual_ind,
    fact_master_remit.receiver_remit_num,
    fact_master_remit.receiver_acct_num_qual_ind,
    fact_master_remit.receiver_acct_num,
    fact_master_remit.dw_last_update_date_time,
    fact_master_remit.source_system_code,
    fact_master_remit.customer_cd,
    fact_master_remit.sender_identifier,
    fact_master_remit.receiver_identifier,
    fact_master_remit.interchange_control_num,
    fact_master_remit.payer_identifier
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_rmt_mirrored_core_dataset_name }}.fact_master_remit
;
