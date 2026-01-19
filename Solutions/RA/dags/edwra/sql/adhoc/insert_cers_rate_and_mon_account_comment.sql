DELETE from {{ params.param_ra_stage_dataset_name }}.cdc_ind where table_name = 'cers_rate_cdc' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.cdc_ind (schema_id,cdc_ind,table_name,job_name,cdc_ind_parm_ind)
VALUES
(1,1,'cers_rate_cdc','NA','Y'),
(3,1,'cers_rate_cdc','NA','Y');

DELETE from {{ params.param_ra_stage_dataset_name }}.cdc_ind where table_name = 'mon_account_comment_cdc' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.cdc_ind (schema_id,cdc_ind,table_name,job_name,cdc_ind_parm_ind)
VALUES
(1,1,'mon_account_comment_cdc','NA','Y'),
(3,1,'mon_account_comment_cdc','NA','Y');

DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'cers_rate_cdc' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'cers_rate_cdc',NULL, '2025-06-05T00:00:00', '2025-06-05T00:00:00', '2025-06-05T00:00:00', NULL),
(3,'cers_rate_cdc',NULL, '2025-06-05T00:00:00','2025-06-05T00:00:00', '2025-06-05T00:00:00', NULL);

DELETE from {{ params.param_ra_stage_dataset_name }}.edw_etl_load where TABLE_NAME = 'mon_account_comment_cdc' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'mon_account_comment_cdc',NULL, '2025-06-05T00:00:00', '2025-06-05T00:00:00', '2025-06-05T00:00:00', NULL),
(3,'mon_account_comment_cdc',NULL, '2025-06-05T00:00:00','2025-06-05T00:00:00', '2025-06-05T00:00:00', NULL);

