
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPECPIDS_PSPECPIDNOTES ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_StgPSPECPIDS_PSPECPIDNOTES (NOTE_CNT, NOTE_DATE, NOTE_TIME, NOTE_TYPE, NOTE_USER, PSPECPIDKEY, PSPECPIDNOTES)
SELECT source.NOTE_CNT, source.NOTE_DATE, source.NOTE_TIME, TRIM(source.NOTE_TYPE), TRIM(source.NOTE_USER), TRIM(source.PSPECPIDKEY), TRIM(source.PSPECPIDNOTES)
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_StgPSPECPIDS_PSPECPIDNOTES as source;
