DELETE from {{ params.param_ra_stage_dataset_name }}.cdc_ind where TABLE_NAME = 'ra_claim_identifier_cdc_gg' and SCHEMA_ID in (1,3);

Insert into {{ params.param_ra_stage_dataset_name }}.cdc_ind (schema_id,cdc_ind,table_name,job_name,cdc_ind_parm_ind)
VALUES
(1,1,'ra_claim_identifier_cdc_gg','NA','Y'),
(3,1,'ra_claim_identifier_cdc_gg','NA','Y');



