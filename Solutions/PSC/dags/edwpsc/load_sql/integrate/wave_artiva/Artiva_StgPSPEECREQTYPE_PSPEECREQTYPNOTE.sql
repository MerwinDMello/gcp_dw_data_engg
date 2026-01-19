
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEECREQTYPE_PSPEECREQTYPNOTE ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPEECREQTYPE_PSPEECREQTYPNOTE (NOTE_CNT, NOTE_DATE, NOTE_TIME, NOTE_TYPE, NOTE_USER, PSPEECREQTYPNOTE, PSPEECRTID)
SELECT source.NOTE_CNT, source.NOTE_DATE, source.NOTE_TIME, TRIM(source.NOTE_TYPE), TRIM(source.NOTE_USER), TRIM(source.PSPEECREQTYPNOTE), TRIM(source.PSPEECRTID)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPEECREQTYPE_PSPEECREQTYPNOTE as source;
