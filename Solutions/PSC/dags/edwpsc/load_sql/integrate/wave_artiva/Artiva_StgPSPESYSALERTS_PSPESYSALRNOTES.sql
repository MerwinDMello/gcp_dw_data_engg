
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESYSALERTS_PSPESYSALRNOTES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPESYSALERTS_PSPESYSALRNOTES (NOTE_CNT, NOTE_DATE, NOTE_TIME, NOTE_TYPE, NOTE_USER, PSPESYSALRKEY, PSPESYSALRNOTES)
SELECT source.NOTE_CNT, source.NOTE_DATE, source.NOTE_TIME, TRIM(source.NOTE_TYPE), TRIM(source.NOTE_USER), TRIM(source.PSPESYSALRKEY), TRIM(source.PSPESYSALRNOTES)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPESYSALERTS_PSPESYSALRNOTES as source;
