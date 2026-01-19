
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTReportingDefinitionsAndSource AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTReportingDefinitionsAndSource AS source
ON target.SRTReportingDefinitionsAndSouceKey = source.SRTReportingDefinitionsAndSouceKey
WHEN MATCHED THEN
  UPDATE SET
  target.SRTReportingDefinitionsAndSouceKey = source.SRTReportingDefinitionsAndSouceKey,
 target.Metric = TRIM(source.Metric),
 target.MetricDefinition = TRIM(source.MetricDefinition),
 target.Criteria = TRIM(source.Criteria),
 target.Report = TRIM(source.Report),
 target.SORT = source.SORT,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (SRTReportingDefinitionsAndSouceKey, Metric, MetricDefinition, Criteria, Report, SORT, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.SRTReportingDefinitionsAndSouceKey, TRIM(source.Metric), TRIM(source.MetricDefinition), TRIM(source.Criteria), TRIM(source.Report), source.SORT, source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SRTReportingDefinitionsAndSouceKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTReportingDefinitionsAndSource
      GROUP BY SRTReportingDefinitionsAndSouceKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTReportingDefinitionsAndSource');
ELSE
  COMMIT TRANSACTION;
END IF;
