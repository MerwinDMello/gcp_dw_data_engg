DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-02-28T08:37:36.456422Z
-- Translation job ID: c1a8ac42-a15b-4c32-9e38-6a123c4e7ba4
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/KJW3Ck/input/clm_fact_claim_physician.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_clm_core_dataset_name }}.fact_claim_physician AS core_claim_physician
USING {{ params.param_clm_stage_dataset_name }}.fact_claim_physician AS stg_claim_physician
ON upper(trim(core_claim_physician.claim_id, ' ')) = upper(trim(stg_claim_physician.claim_id, ' '))
AND upper(trim(core_claim_physician.phys_type_code, ' ')) = upper(trim(stg_claim_physician.phys_type_code, ' '))
AND upper(trim(core_claim_physician.phys_qual_code, ' ')) = upper(trim(stg_claim_physician.phys_qual_code, ' '))
WHEN MATCHED THEN
UPDATE
SET dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    phys_code = TRIM(stg_claim_physician.phys_code),
    phys_last_name = TRIM(stg_claim_physician.phys_last_name),
    phys_first_name = TRIM(stg_claim_physician.phys_first_name),
    phys_taxonomy_code = TRIM(stg_claim_physician.phys_taxonomy_code)
WHEN NOT MATCHED BY TARGET THEN
INSERT (claim_id,
        phys_type_code,
        phys_qual_code,
        phys_code,
        phys_last_name,
        phys_first_name,
        phys_taxonomy_code,
        dw_last_update_date_time,
        source_system_code)
VALUES (TRIM(stg_claim_physician.claim_id), TRIM(stg_claim_physician.phys_type_code), TRIM(stg_claim_physician.phys_qual_code),
TRIM(stg_claim_physician.phys_code), TRIM(stg_claim_physician.phys_last_name), TRIM(stg_claim_physician.phys_first_name),
TRIM(stg_claim_physician.phys_taxonomy_code), datetime_trunc(current_datetime('US/Central'), SECOND), TRIM(stg_claim_physician.source_system_code));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT claim_id,
             phys_type_code,
             phys_qual_code
      FROM {{ params.param_clm_core_dataset_name }}.fact_claim_physician
      GROUP BY claim_id,
               phys_type_code,
               phys_qual_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_clm_core_dataset_name }}.fact_claim_physician');

ELSE
COMMIT TRANSACTION;

END IF;