DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'ra_claim_category_cdc' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'ra_claim_category_cdc',NULL, '2025-04-17T00:00:00', '2025-04-17T00:00:00', '2025-04-17T00:00:00', NULL),
(3,'ra_claim_category_cdc',NULL, '2025-04-17T00:00:00', '2025-04-17T00:00:00', '2025-04-17T00:00:00', NULL);

DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'ra_service_category_cdc' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'ra_service_category_cdc',NULL, '2025-04-17T00:00:00', '2025-04-17T00:00:00', '2025-04-17T00:00:00', NULL),
(3,'ra_service_category_cdc',NULL, '2025-04-17T00:00:00', '2025-04-17T00:00:00', '2025-04-17T00:00:00', NULL);