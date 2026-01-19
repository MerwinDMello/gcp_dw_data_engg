
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCUPLTRANS ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSOCUPLTRANS (PSOCTCATEGORY, PSOCTCRTDTE, PSOCTCRTTIME, PSOCTEXPDTE, PSOCTFIELDID, PSOCTFROM, PSOCTKEY, PSOCTOCID, PSOCTTO)
SELECT TRIM(source.PSOCTCATEGORY), source.PSOCTCRTDTE, source.PSOCTCRTTIME, source.PSOCTEXPDTE, TRIM(source.PSOCTFIELDID), TRIM(source.PSOCTFROM), source.PSOCTKEY, source.PSOCTOCID, TRIM(source.PSOCTTO)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSOCUPLTRANS as source;
