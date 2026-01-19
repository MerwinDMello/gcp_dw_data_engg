
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSRATEREFERENCE ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSRATEREFERENCE (PSRCOID, PSRDEPT, PSRFINCLASS, PSRKEY, PSRPROCFLG, PSRRATE)
SELECT TRIM(source.PSRCOID), TRIM(source.PSRDEPT), TRIM(source.PSRFINCLASS), TRIM(source.PSRKEY), TRIM(source.PSRPROCFLG), TRIM(source.PSRRATE)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSRATEREFERENCE as source;
