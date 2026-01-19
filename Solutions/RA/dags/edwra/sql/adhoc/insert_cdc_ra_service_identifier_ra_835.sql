
DELETE from {{ params.param_ra_stage_dataset_name }}.cdc_ind where TABLE_NAME = 'cers_drg_rate_cdc_gg' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.cdc_ind (schema_id,cdc_ind,table_name,job_name,cdc_ind_parm_ind)
VALUES
(1,1,'cers_drg_rate_cdc_gg','NA','Y'),
(3,1,'cers_drg_rate_cdc_gg','NA','Y');

DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'cers_drg_rate_cdc_gg' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'cers_drg_rate_cdc_gg',NULL, '2025-06-15T02:00:00', '2025-06-15T02:00:00', NULL, NULL),
(3,'cers_drg_rate_cdc_gg',NULL, '2025-06-15T02:00:00','2025-06-15T02:00:00', NULL, NULL);



DELETE from {{ params.param_ra_stage_dataset_name }}.cdc_ind where TABLE_NAME = 'ra_835_category_aggregated_cdc_gg' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.cdc_ind (schema_id,cdc_ind,table_name,job_name,cdc_ind_parm_ind)
VALUES
(1,1,'ra_835_category_aggregated_cdc_gg','NA','Y'),
(3,1,'ra_835_category_aggregated_cdc_gg','NA','Y');

DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'ra_835_category_aggregated_cdc_gg' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'ra_835_category_aggregated_cdc_gg',NULL, '2025-06-15T02:00:00', '2025-06-15T02:00:00', NULL, NULL),
(3,'ra_835_category_aggregated_cdc_gg',NULL, '2025-06-15T02:00:00','2025-06-15T02:00:00', NULL, NULL);


DELETE from {{ params.param_ra_stage_dataset_name }}.cdc_ind where TABLE_NAME = 'ra_service_identifier_cdc_gg' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.cdc_ind (schema_id,cdc_ind,table_name,job_name,cdc_ind_parm_ind)
VALUES
(1,1,'ra_service_identifier_cdc_gg','NA','Y'),
(3,1,'ra_service_identifier_cdc_gg','NA','Y');

DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'ra_service_identifier_cdc_gg' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'ra_service_identifier_cdc_gg',NULL, '2025-06-15T02:00:00', '2025-06-15T02:00:00', NULL, NULL),
(3,'ra_service_identifier_cdc_gg',NULL, '2025-06-15T02:00:00','2025-06-15T02:00:00', NULL, NULL);
