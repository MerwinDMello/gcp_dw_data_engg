DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-02-28T08:37:36.456422Z
-- Translation job ID: c1a8ac42-a15b-4c32-9e38-6a123c4e7ba4
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/KJW3Ck/input/clm_lu_billing_provider.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_clm_core_dataset_name }}.lu_billing_provider AS core_lu_billing_provider
USING {{ params.param_clm_stage_dataset_name }}.lu_billing_provider AS stg_lu_billing_provider
ON core_lu_billing_provider.bill_provider_sid = stg_lu_billing_provider.bill_provider_sid
WHEN MATCHED THEN
UPDATE
SET dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    bill_provider_name = TRIM(stg_lu_billing_provider.bill_provider_name),
    bill_provider_addr1 = TRIM(stg_lu_billing_provider.bill_provider_addr1),
    bill_provider_addr2 = TRIM(stg_lu_billing_provider.bill_provider_addr2),
    bill_provider_city = TRIM(stg_lu_billing_provider.bill_provider_city),
    bill_provider_st = TRIM(stg_lu_billing_provider.bill_provider_st),
    bill_provider_zip_cd = TRIM(stg_lu_billing_provider.bill_provider_zip_cd),
    bill_provider_npi = stg_lu_billing_provider.bill_provider_npi
WHEN NOT MATCHED BY TARGET THEN
INSERT (bill_provider_sid,
        bill_provider_name,
        bill_provider_addr1,
        bill_provider_addr2,
        bill_provider_city,
        bill_provider_st,
        bill_provider_zip_cd,
        bill_provider_npi,
        dw_last_update_date_time,
        source_system_code)
VALUES (stg_lu_billing_provider.bill_provider_sid, TRIM(stg_lu_billing_provider.bill_provider_name),
TRIM(stg_lu_billing_provider.bill_provider_addr1), TRIM(stg_lu_billing_provider.bill_provider_addr2),
TRIM(stg_lu_billing_provider.bill_provider_city), TRIM(stg_lu_billing_provider.bill_provider_st),
TRIM(stg_lu_billing_provider.bill_provider_zip_cd), stg_lu_billing_provider.bill_provider_npi, 
datetime_trunc(current_datetime('US/Central'), SECOND), TRIM(stg_lu_billing_provider.source_system_code));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT bill_provider_sid
      FROM {{ params.param_clm_core_dataset_name }}.lu_billing_provider
      GROUP BY bill_provider_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_clm_core_dataset_name }}.lu_billing_provider');

ELSE
COMMIT TRANSACTION;

END IF;