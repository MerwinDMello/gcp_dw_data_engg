DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_fact_patient_qual.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_rmt_core_dataset_name }}.fact_patient_qual AS pfpq
USING {{ params.param_rmt_stage_dataset_name }}.fact_patient_qual AS sfpq
ON UPPER(TRIM(pfpq.patient_remit_sid, ' ')) = UPPER(TRIM(sfpq.patient_remit_sid, ' '))
AND pfpq.qual_seq_num = sfpq.qual_seq_num
AND pfpq.svc_line_seq_num = sfpq.svc_line_seq_num
WHEN MATCHED THEN
UPDATE
SET qualifier_code = TRIM(sfpq.qualifier_code),
    pat_qual_id = TRIM(sfpq.pat_qual_id),
    pat_qual_ind = TRIM(sfpq.pat_qual_ind),
    pat_qual_name = TRIM(sfpq.pat_qual_name),
    pat_qual_amt = sfpq.pat_qual_amt,
    pat_qual_code = TRIM(sfpq.pat_qual_code),
    pat_qual_cnt = sfpq.pat_qual_cnt,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = TRIM(sfpq.source_system_code),
    customer_cd = TRIM(sfpq.customer_cd)
WHEN NOT MATCHED BY TARGET THEN
INSERT (qual_seq_num,
        patient_remit_sid,
        svc_line_seq_num,
        qualifier_code,
        pat_qual_id,
        pat_qual_ind,
        pat_qual_name,
        pat_qual_amt,
        pat_qual_code,
        pat_qual_cnt,
        dw_last_update_date_time,
        source_system_code,
        customer_cd)
VALUES (sfpq.qual_seq_num, TRIM(sfpq.patient_remit_sid), sfpq.svc_line_seq_num, TRIM(sfpq.qualifier_code),
TRIM(sfpq.pat_qual_id), TRIM(sfpq.pat_qual_ind), TRIM(sfpq.pat_qual_name), sfpq.pat_qual_amt,
TRIM(sfpq.pat_qual_code), sfpq.pat_qual_cnt, datetime_trunc(current_datetime('US/Central'), SECOND),
TRIM(sfpq.source_system_code), TRIM(sfpq.customer_cd));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT qual_seq_num,
             patient_remit_sid,
             svc_line_seq_num
      FROM {{ params.param_rmt_core_dataset_name }}.fact_patient_qual
      GROUP BY qual_seq_num,
               patient_remit_sid,
               svc_line_seq_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_rmt_core_dataset_name }}.fact_patient_qual');

ELSE
COMMIT TRANSACTION;

END IF;