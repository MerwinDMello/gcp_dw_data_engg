
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYER_PSPEPAYNOTES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEPAYER_PSPEPAYNOTES (NOTE_CNT, NOTE_DATE, NOTE_TIME, NOTE_TYPE, NOTE_USER, PSPEPAYKEY, PSPEPAYNOTES)
SELECT source.NOTE_CNT, source.NOTE_DATE, source.NOTE_TIME, TRIM(source.NOTE_TYPE), TRIM(source.NOTE_USER), TRIM(source.PSPEPAYKEY), TRIM(source.PSPEPAYNOTES)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEPAYER_PSPEPAYNOTES as source;
