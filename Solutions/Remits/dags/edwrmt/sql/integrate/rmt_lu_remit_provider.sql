DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_lu_remit_provider.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_rmt_core_dataset_name }}.lu_remit_provider AS pluprv
USING {{ params.param_rmt_stage_dataset_name }}.lu_remit_provider AS sluprv
ON UPPER(TRIM(pluprv.remit_provider_id, ' ')) = UPPER(TRIM(sluprv.remit_provider_id, ' '))
WHEN MATCHED THEN
UPDATE
SET provider_name = TRIM(sluprv.provider_name),
    address_1 = TRIM(sluprv.address_1),
    address_2 = TRIM(sluprv.address_2),
    city = TRIM(sluprv.city),
    state = TRIM(sluprv.state),
    zip = TRIM(sluprv.zip),
    country_cd = TRIM(sluprv.country_cd),
    npi = TRIM(sluprv.npi),
    tax_id = TRIM(sluprv.tax_id),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = TRIM(sluprv.source_system_code),
    customer_cd = TRIM(sluprv.customer_cd)
WHEN NOT MATCHED BY TARGET THEN
INSERT (remit_provider_id,
        provider_name,
        address_1,
        address_2,
        city,
        state,
        zip,
        country_cd,
        npi,
        tax_id,
        dw_last_update_date_time,
        source_system_code,
        customer_cd)
VALUES (TRIM(sluprv.remit_provider_id), TRIM(sluprv.provider_name), TRIM(sluprv.address_1),
TRIM(sluprv.address_2), TRIM(sluprv.city), TRIM(sluprv.state), TRIM(sluprv.zip), TRIM(sluprv.country_cd),
TRIM(sluprv.npi), TRIM(sluprv.tax_id), datetime_trunc(current_datetime('US/Central'), SECOND),
TRIM(sluprv.source_system_code), TRIM(sluprv.customer_cd));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT remit_provider_id
      FROM {{ params.param_rmt_core_dataset_name }}.lu_remit_provider
      GROUP BY remit_provider_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_rmt_core_dataset_name }}.lu_remit_provider');

ELSE
COMMIT TRANSACTION;

END IF;