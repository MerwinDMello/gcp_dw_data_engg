DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

-- Translation time: 2025-03-04T09:56:02.533191Z
-- Translation job ID: b76396e3-185c-4254-a4c8-8cf665487226
-- Source: gs://eim-parallon-cs-datamig-dev-0002/clmrmt_bteqs_bulk_conversion/B1xZGH/input/rmt_lu_remit_payer.sql
-- Translated from: Teradata
-- Translated to: BigQuery

MERGE INTO {{ params.param_rmt_core_dataset_name }}.lu_remit_payer AS plupay
USING {{ params.param_rmt_stage_dataset_name }}.lu_remit_payer AS slupay
ON UPPER(TRIM(plupay.remit_payer_id, ' ')) = UPPER(TRIM(slupay.remit_payer_id, ' '))
WHEN MATCHED THEN
UPDATE
SET payer_name = TRIM(slupay.payer_name),
    address_1 = TRIM(slupay.address_1),
    address_2 = TRIM(slupay.address_2),
    city = TRIM(slupay.city),
    state = TRIM(slupay.state),
    zip = TRIM(slupay.zip),
    country_cd = TRIM(slupay.country_cd),
    payer_id_num = TRIM(slupay.payer_id_num),
    source_system_code = TRIM(slupay.source_system_code),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    customer_cd = TRIM(slupay.customer_cd)
WHEN NOT MATCHED BY TARGET THEN
INSERT (remit_payer_id,
        payer_name,
        address_1,
        address_2,
        city,
        state,
        zip,
        country_cd,
        payer_id_num,
        source_system_code,
        dw_last_update_date_time,
        customer_cd)
VALUES (TRIM(slupay.remit_payer_id), TRIM(slupay.payer_name), TRIM(slupay.address_1), 
TRIM(slupay.address_2), TRIM(slupay.city), TRIM(slupay.state), TRIM(slupay.zip), TRIM(slupay.country_cd),
TRIM(slupay.payer_id_num), TRIM(slupay.source_system_code),
datetime_trunc(current_datetime('US/Central'), SECOND), TRIM(slupay.customer_cd));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT remit_payer_id
      FROM {{ params.param_rmt_core_dataset_name }}.lu_remit_payer
      GROUP BY remit_payer_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_rmt_core_dataset_name }}.lu_remit_payer');

ELSE
COMMIT TRANSACTION;

END IF;