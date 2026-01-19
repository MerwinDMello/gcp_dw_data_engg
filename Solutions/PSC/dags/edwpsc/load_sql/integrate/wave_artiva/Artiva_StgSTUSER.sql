
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgSTUSER ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgSTUSER (USERID, UAFIRST, UALAST, UAFULLNAME, HCUATITLE, HCUADEPT, UAOFFICE, UASSODOMAIN)
SELECT TRIM(source.USERID), TRIM(source.UAFIRST), TRIM(source.UALAST), TRIM(source.UAFULLNAME), TRIM(source.HCUATITLE), TRIM(source.HCUADEPT), TRIM(source.UAOFFICE), TRIM(source.UASSODOMAIN)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgSTUSER as source;
