
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCLINKS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCLINKS (PSOCLICOUNT, PSOCLIDATE, PSOCLIDESC, PSOCLILEADMSG, PSOCLIUSER, PSOCLIUSERCHGDTE, PSOCLIKEY)
SELECT source.PSOCLICOUNT, source.PSOCLIDATE, TRIM(source.PSOCLIDESC), source.PSOCLILEADMSG, TRIM(source.PSOCLIUSER), source.PSOCLIUSERCHGDTE, source.PSOCLIKEY
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSOCLINKS as source;
