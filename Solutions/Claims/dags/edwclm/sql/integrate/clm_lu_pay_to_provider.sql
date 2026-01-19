DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-02-28T08:37:36.456422Z
-- Translation job ID: c1a8ac42-a15b-4c32-9e38-6a123c4e7ba4
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/KJW3Ck/input/clm_lu_pay_to_provider.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_clm_core_dataset_name }}.lu_pay_to_provider AS core_lu_provider
USING {{ params.param_clm_stage_dataset_name }}.lu_pay_to_provider AS stg_lu_provider
ON core_lu_provider.pay_to_provider_sid = stg_lu_provider.pay_to_provider_sid
WHEN MATCHED THEN
UPDATE
SET dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    pay_to_provider_name = TRIM(stg_lu_provider.pay_to_provider_name),
    pay_to_provider_addr1 = TRIM(stg_lu_provider.pay_to_provider_addr1),
    pay_to_provider_addr2 = TRIM(stg_lu_provider.pay_to_provider_addr2),
    pay_to_provider_city = TRIM(stg_lu_provider.pay_to_provider_city),
    pay_to_provider_st = TRIM(stg_lu_provider.pay_to_provider_st),
    pay_to_provider_zip_cd = TRIM(stg_lu_provider.pay_to_provider_zip_cd)
WHEN NOT MATCHED BY TARGET THEN
INSERT (pay_to_provider_sid,
        pay_to_provider_name,
        pay_to_provider_addr1,
        pay_to_provider_addr2,
        pay_to_provider_city,
        pay_to_provider_st,
        pay_to_provider_zip_cd,
        dw_last_update_date_time,
        source_system_code)
VALUES (stg_lu_provider.pay_to_provider_sid, TRIM(stg_lu_provider.pay_to_provider_name),
TRIM(stg_lu_provider.pay_to_provider_addr1), TRIM(stg_lu_provider.pay_to_provider_addr2),
TRIM(stg_lu_provider.pay_to_provider_city), TRIM(stg_lu_provider.pay_to_provider_st),
TRIM(stg_lu_provider.pay_to_provider_zip_cd), datetime_trunc(current_datetime('US/Central'), SECOND),
TRIM(stg_lu_provider.source_system_code));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT pay_to_provider_sid
      FROM {{ params.param_clm_core_dataset_name }}.lu_pay_to_provider
      GROUP BY pay_to_provider_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_clm_core_dataset_name }}.lu_pay_to_provider');

ELSE
COMMIT TRANSACTION;

END IF;