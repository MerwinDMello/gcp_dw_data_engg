
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_RefLineCnt ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_RefLineCnt (LineCnt, LineDesc, LineSubGroup, SourceARecordLastUpdated, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, LineSubGroupOrder)
SELECT source.LineCnt, TRIM(source.LineDesc), TRIM(source.LineSubGroup), source.SourceARecordLastUpdated, source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), source.LineSubGroupOrder
FROM {{ params.param_psc_stage_dataset_name }}.ECW_RefLineCnt as source;
