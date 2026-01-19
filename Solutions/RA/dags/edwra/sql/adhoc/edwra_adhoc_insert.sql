Insert into edwra_staging.adhoc_ind (schema_id,adhoc_ind,table_name,job_name,adhoc_ind_parm_ind)
VALUES
(1,0,'mon_account_comment_adhoc','NA','Y'),
(3,0,'mon_account_comment_adhoc','NA','Y');

Insert into edwra_staging.adhoc_ind (schema_id,adhoc_ind,table_name,job_name,adhoc_ind_parm_ind)
VALUES
(1,0,'cers_rate_adhoc','NA','Y'),
(3,0,'cers_rate_adhoc','NA','Y');

Insert into edwra_staging.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'mon_account_comment','2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:00', NULL),
(3,'mon_account_comment','2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:00', NULL); 

Insert into edwra_staging.edw_etl_load (SCHEMA_ID,TABLE_NAME,LAST_EFF_FROM_DATE_TIME,LAST_EFF_TO_DATE_TIME,CURRENT_EFF_FROM_DATE_TIME,CURRENT_EFF_TO_DATE_TIME,LAST_UPDATE_DATE_TIME)
VALUES
(1,'cers_rate','2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:00', NULL),
(3,'cers_rate','2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:00', NULL); 