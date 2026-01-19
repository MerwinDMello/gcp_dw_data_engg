DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_fact_patient_remit_adj.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_rmt_core_dataset_name }}.fact_patient_remit_adj AS pfpra
USING {{ params.param_rmt_stage_dataset_name }}.fact_patient_remit_adj AS sfpra
ON UPPER(TRIM(pfpra.patient_remit_sid, ' ')) = UPPER(TRIM(sfpra.patient_remit_sid, ' '))
AND pfpra.adj_seq_num = sfpra.adj_seq_num
WHEN MATCHED THEN
UPDATE
SET claim_adj_group_cd = TRIM(sfpra.claim_adj_group_cd),
    claim_adj_reason_cd = TRIM(sfpra.claim_adj_reason_cd),
    adjustment_amt = sfpra.adjustment_amt,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = TRIM(sfpra.source_system_code),
    adjustment_quantity = sfpra.adjustment_quantity,
    customer_cd = TRIM(sfpra.customer_cd) 
WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_remit_sid,
        adj_seq_num,
        claim_adj_group_cd,
        claim_adj_reason_cd,
        adjustment_amt,
        dw_last_update_date_time,
        source_system_code,
        adjustment_quantity,
        customer_cd)
VALUES (TRIM(sfpra.patient_remit_sid), sfpra.adj_seq_num, TRIM(sfpra.claim_adj_group_cd),
TRIM(sfpra.claim_adj_reason_cd), sfpra.adjustment_amt, datetime_trunc(current_datetime('US/Central'), SECOND),
TRIM(sfpra.source_system_code), sfpra.adjustment_quantity, TRIM(sfpra.customer_cd));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_remit_sid,
             adj_seq_num
      FROM {{ params.param_rmt_core_dataset_name }}.fact_patient_remit_adj
      GROUP BY patient_remit_sid,
               adj_seq_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_rmt_core_dataset_name }}.fact_patient_remit_adj');

ELSE
COMMIT TRANSACTION;

END IF;