INSERT INTO hca-hin-prod-cur-parallon.`edwra_staging.ra_835_category_aggregated` 
(id, schema_id, ra_claim_payment_id, ra_category_id, amount, dw_last_update_date)
SELECT id, schema_id, ra_claim_payment_id, ra_category_id, amount, dw_last_update_date
FROM hca-hin-qa-cur-parallon.`edwra_staging.ra_835_category_aggregated` ;

UPdate edwra_staging.edw_etl_load 
set LAST_EFF_TO_DATE_TIME = '2025-07-28T00:00:00',
    CURRENT_EFF_TO_DATE_TIME = '2025-07-28T00:00:00'
where lower(table_name) like 'ra_835_category_aggregated_cdc_gg%';