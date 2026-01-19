DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_fact_master_remit.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_rmt_core_dataset_name }}.fact_master_remit AS fmr
USING {{ params.param_rmt_stage_dataset_name }}.fact_master_remit AS sfmr
ON UPPER(TRIM(fmr.remit_id, ' ')) = UPPER(TRIM(sfmr.remit_id, ' '))
WHEN MATCHED THEN
UPDATE
SET remit_entered_dt = sfmr.remit_entered_dt,
    remit_effective_dt = sfmr.remit_effective_dt,
    remit_total_amt = sfmr.remit_total_amt,
    check_num = TRIM(sfmr.check_num),
    payment_type_ind = TRIM(sfmr.payment_type_ind),
    tran_handle_cd = TRIM(sfmr.tran_handle_cd),
    credit_debit_ind = TRIM(sfmr.credit_debit_ind),
    pay_method_cd = TRIM(sfmr.pay_method_cd),
    pay_fmt_cd = TRIM(sfmr.pay_fmt_cd),
    sender_remit_id_qual_ind = TRIM(sfmr.sender_remit_id_qual_ind),
    sender_remit_num = sfmr.sender_remit_num,
    sender_acct_num = TRIM(sfmr.sender_acct_num),
    sender_acct_num_qual_ind = TRIM(sfmr.sender_acct_num_qual_ind),
    receiver_remit_id_qual_ind = TRIM(sfmr.receiver_remit_id_qual_ind),
    receiver_remit_num = TRIM(sfmr.receiver_remit_num),
    receiver_acct_num_qual_ind = TRIM(sfmr.receiver_acct_num_qual_ind),
    receiver_acct_num = TRIM(sfmr.receiver_acct_num),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = TRIM(sfmr.source_system_code),
    customer_cd = TRIM(sfmr.customer_cd),
    sender_identifier = TRIM(sfmr.sender_identifier),
    receiver_identifier = TRIM(sfmr.receiver_identifier),
    interchange_control_num = TRIM(sfmr.interchange_control_num),
    payer_identifier = TRIM(sfmr.payer_identifier)
WHEN NOT MATCHED BY TARGET THEN
INSERT (remit_id,
        remit_entered_dt,
        remit_effective_dt,
        remit_total_amt,
        check_num,
        payment_type_ind,
        tran_handle_cd,
        credit_debit_ind,
        pay_method_cd,
        pay_fmt_cd,
        sender_remit_id_qual_ind,
        sender_remit_num,
        sender_acct_num,
        sender_acct_num_qual_ind,
        receiver_remit_id_qual_ind,
        receiver_remit_num,
        receiver_acct_num_qual_ind,
        receiver_acct_num,
        dw_last_update_date_time,
        source_system_code,
        customer_cd,
        sender_identifier,
        receiver_identifier,
        interchange_control_num,
        payer_identifier)
VALUES (TRIM(sfmr.remit_id), sfmr.remit_entered_dt, sfmr.remit_effective_dt, sfmr.remit_total_amt, 
TRIM(sfmr.check_num), TRIM(sfmr.payment_type_ind), TRIM(sfmr.tran_handle_cd), TRIM(sfmr.credit_debit_ind),
TRIM(sfmr.pay_method_cd), TRIM(sfmr.pay_fmt_cd), TRIM(sfmr.sender_remit_id_qual_ind), sfmr.sender_remit_num,
TRIM(sfmr.sender_acct_num), TRIM(sfmr.sender_acct_num_qual_ind), TRIM(sfmr.receiver_remit_id_qual_ind),
TRIM(sfmr.receiver_remit_num), TRIM(sfmr.receiver_acct_num_qual_ind), TRIM(sfmr.receiver_acct_num),
datetime_trunc(current_datetime('US/Central'), SECOND), TRIM(sfmr.source_system_code),
TRIM(sfmr.customer_cd), TRIM(sfmr.sender_identifier), TRIM(sfmr.receiver_identifier),
TRIM(sfmr.interchange_control_num), TRIM(sfmr.payer_identifier));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT remit_id
      FROM {{ params.param_rmt_core_dataset_name }}.fact_master_remit
      GROUP BY remit_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_rmt_core_dataset_name }}.fact_master_remit');

ELSE
COMMIT TRANSACTION;

END IF;