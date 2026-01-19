Insert into {{ params.param_ra_stage_dataset_name }}.cdc_ind (schema_id,cdc_ind,table_name,job_name,cdc_ind_parm_ind)
VALUES
(1,1,'ra_claim_category_cdc','NA','Y'),
(3,1,'ra_claim_category_cdc','NA','Y');

Insert into {{ params.param_ra_stage_dataset_name }}.cdc_ind (schema_id,cdc_ind,table_name,job_name,cdc_ind_parm_ind)
VALUES
(1,1,'ra_service_category_cdc','NA','Y'),
(3,1,'ra_service_category_cdc','NA','Y');