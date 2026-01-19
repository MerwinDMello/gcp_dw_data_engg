DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-02-28T08:37:36.456422Z
-- Translation job ID: c1a8ac42-a15b-4c32-9e38-6a123c4e7ba4
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/KJW3Ck/input/clm_fact_code.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_clm_core_dataset_name }}.fact_code AS core_fact_code
USING {{ params.param_clm_stage_dataset_name }}.fact_code AS stg_fact_code
ON upper(trim(core_fact_code.claim_id, ' ')) = upper(trim(stg_fact_code.claim_id, ' '))
AND core_fact_code.code_seq_num = stg_fact_code.code_seq_num
AND upper(trim(core_fact_code.code_type_id, ' ')) = upper(trim(stg_fact_code.code_type_id, ' '))
WHEN MATCHED THEN
UPDATE
SET dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    code_value = TRIM(stg_fact_code.code_value),
    code_amt = stg_fact_code.code_amt,
    code_from_dt = stg_fact_code.code_from_dt,
    code_thru_dt = stg_fact_code.code_thru_dt,
    code_poa_ind = TRIM(stg_fact_code.code_poa_ind)
WHEN NOT MATCHED BY TARGET THEN
INSERT (claim_id,
        code_seq_num,
        code_type_id,
        code_value,
        code_amt,
        code_from_dt,
        code_thru_dt,
        code_poa_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (TRIM(stg_fact_code.claim_id), stg_fact_code.code_seq_num, TRIM(stg_fact_code.code_type_id),
stg_fact_code.code_value, stg_fact_code.code_amt, stg_fact_code.code_from_dt, stg_fact_code.code_thru_dt,
TRIM(stg_fact_code.code_poa_ind), datetime_trunc(current_datetime('US/Central'), SECOND),
TRIM(stg_fact_code.source_system_code));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT claim_id,
             code_seq_num,
             code_type_id
      FROM {{ params.param_clm_core_dataset_name }}.fact_code
      GROUP BY claim_id,
               code_seq_num,
               code_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_clm_core_dataset_name }}.fact_code');

ELSE
COMMIT TRANSACTION;

END IF;