
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefRHStatusCtgy ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefRHStatusCtgy (StatusCtgyCode, StatusCtgyDesc)
SELECT TRIM(source.StatusCtgyCode), TRIM(source.StatusCtgyDesc)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefRHStatusCtgy as source;
