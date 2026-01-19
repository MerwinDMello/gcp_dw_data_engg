
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefArtivaPool ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefArtivaPool (ArtivaPoolKey, ArtivaPoolDesc)
SELECT TRIM(source.ArtivaPoolKey), TRIM(source.ArtivaPoolDesc)
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefArtivaPool as source;
