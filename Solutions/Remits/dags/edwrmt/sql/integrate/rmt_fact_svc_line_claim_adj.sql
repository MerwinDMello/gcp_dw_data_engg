DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_fact_svc_line_claim_adj.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_rmt_core_dataset_name }}.fact_svc_line_claim_adj AS pfslca
USING {{ params.param_rmt_stage_dataset_name }}.fact_svc_line_claim_adj AS sfslca
ON UPPER(TRIM(pfslca.patient_remit_sid, ' ')) = UPPER(TRIM(sfslca.patient_remit_sid, ' '))
AND pfslca.svc_line_seq_num = sfslca.svc_line_seq_num
AND pfslca.line_clm_adj_seq_num = sfslca.line_clm_adj_seq_num
WHEN MATCHED THEN
UPDATE
SET claim_adj_group_cd = TRIM(sfslca.claim_adj_group_cd),
    claim_adj_reason_cd = TRIM(sfslca.claim_adj_reason_cd),
    adj_amt = sfslca.adj_amt,
    adj_qty_num = sfslca.adj_qty_num,
    dw_last_update_date_time = current_datetime('US/Central'),
    source_system_code = TRIM(sfslca.source_system_code),
    customer_cd = TRIM(sfslca.customer_cd)
WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_remit_sid,
        svc_line_seq_num,
        line_clm_adj_seq_num,
        claim_adj_group_cd,
        claim_adj_reason_cd,
        adj_amt,
        adj_qty_num,
        dw_last_update_date_time,
        source_system_code,
        customer_cd)
VALUES (TRIM(sfslca.patient_remit_sid), sfslca.svc_line_seq_num, sfslca.line_clm_adj_seq_num,
TRIM(sfslca.claim_adj_group_cd), TRIM(sfslca.claim_adj_reason_cd), sfslca.adj_amt, sfslca.adj_qty_num,
current_datetime('US/Central'), TRIM(sfslca.source_system_code), TRIM(sfslca.customer_cd));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_remit_sid,
             svc_line_seq_num,
             line_clm_adj_seq_num
      FROM {{ params.param_rmt_core_dataset_name }}.fact_svc_line_claim_adj
      GROUP BY patient_remit_sid,
               svc_line_seq_num,
               line_clm_adj_seq_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_rmt_core_dataset_name }}.fact_svc_line_claim_adj');

ELSE
COMMIT TRANSACTION;

END IF;