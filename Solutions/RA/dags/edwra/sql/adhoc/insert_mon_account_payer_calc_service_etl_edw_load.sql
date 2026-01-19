DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'Mon_Account_Payer_Calc_Service' and SCHEMA_ID = 1;

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'Mon_Account_Payer_Calc_Service',NULL, '2025-02-26T03:28:25', '2025-02-26T03:28:25', '2025-02-26T03:28:25', NULL);


