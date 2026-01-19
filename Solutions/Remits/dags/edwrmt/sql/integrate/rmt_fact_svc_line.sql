DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_fact_svc_line.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_rmt_core_dataset_name }}.fact_svc_line AS pfsl
USING {{ params.param_rmt_stage_dataset_name }}.fact_svc_line AS sfsl
ON UPPER(TRIM(pfsl.patient_remit_sid, ' ')) = UPPER(TRIM(sfsl.patient_remit_sid, ' '))
AND pfsl.svc_line_seq_num = sfsl.svc_line_seq_num
WHEN MATCHED THEN
UPDATE
SET svc_line_cd = TRIM(sfsl.svc_line_cd),
    service_line_dt = sfsl.service_line_dt,
    service_end_dt = sfsl.service_end_dt,
    serv_line_charge_amt = sfsl.serv_line_charge_amt,
    serv_line_payment_amt = sfsl.serv_line_payment_amt,
    rev_code = TRIM(sfsl.rev_code),
    unit_of_svc_paid_cnt = sfsl.unit_of_svc_paid_cnt,
    submitted_service_cd = TRIM(sfsl.submitted_service_cd),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = TRIM(sfsl.source_system_code),
    svc_line_cd_qual = TRIM(sfsl.svc_line_cd_qual),
    proc_mod_1 = TRIM(sfsl.proc_mod_1),
    proc_mod_2 = TRIM(sfsl.proc_mod_2),
    proc_mod_3 = TRIM(sfsl.proc_mod_3),
    proc_mod_4 = TRIM(sfsl.proc_mod_4),
    customer_cd = TRIM(sfsl.customer_cd)
WHEN NOT MATCHED BY TARGET THEN
INSERT (svc_line_seq_num,
        patient_remit_sid,
        svc_line_cd,
        service_line_dt,
        service_end_dt,
        serv_line_charge_amt,
        serv_line_payment_amt,
        rev_code,
        unit_of_svc_paid_cnt,
        submitted_service_cd,
        dw_last_update_date_time,
        source_system_code,
        svc_line_cd_qual,
        proc_mod_1,
        proc_mod_2,
        proc_mod_3,
        proc_mod_4,
        customer_cd)
VALUES (sfsl.svc_line_seq_num, TRIM(sfsl.patient_remit_sid), TRIM(sfsl.svc_line_cd), sfsl.service_line_dt,
sfsl.service_end_dt, sfsl.serv_line_charge_amt, sfsl.serv_line_payment_amt, TRIM(sfsl.rev_code),
sfsl.unit_of_svc_paid_cnt, TRIM(sfsl.submitted_service_cd), datetime_trunc(current_datetime('US/Central'),
SECOND), TRIM(sfsl.source_system_code), TRIM(sfsl.svc_line_cd_qual), TRIM(sfsl.proc_mod_1),
TRIM(sfsl.proc_mod_2), TRIM(sfsl.proc_mod_3), TRIM(sfsl.proc_mod_4), TRIM(sfsl.customer_cd));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_remit_sid,
             svc_line_seq_num
      FROM {{ params.param_rmt_core_dataset_name }}.fact_svc_line
      GROUP BY patient_remit_sid,
               svc_line_seq_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_rmt_core_dataset_name }}.fact_svc_line');

ELSE
COMMIT TRANSACTION;

END IF;