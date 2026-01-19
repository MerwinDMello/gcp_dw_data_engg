DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'RA_Claim_Identifier_CDC_GG' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'RA_Claim_Identifier_CDC_GG',NULL, '2025-02-20T02:00:00', '2025-02-20T02:00:00', NULL, NULL),
(3,'RA_Claim_Identifier_CDC_GG',NULL, '2025-02-20T02:00:00','2025-02-20T02:00:00', NULL, NULL);


