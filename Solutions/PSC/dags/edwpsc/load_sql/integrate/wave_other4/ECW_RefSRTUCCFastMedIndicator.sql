
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTUCCFastMedIndicator AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTUCCFastMedIndicator AS source
ON target.UCCFastMedIndicatorKey = source.UCCFastMedIndicatorKey
WHEN MATCHED THEN
  UPDATE SET
  target.UCCFastMedIndicatorKey = source.UCCFastMedIndicatorKey,
 target.ID = source.ID,
 target.Practice = TRIM(source.Practice),
 target.Clinic = TRIM(source.Clinic),
 target.FastMedIndicator = TRIM(source.FastMedIndicator),
 target.EffectiveDate = source.EffectiveDate,
 target.TermedDate = source.TermedDate,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (UCCFastMedIndicatorKey, ID, Practice, Clinic, FastMedIndicator, EffectiveDate, TermedDate, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.UCCFastMedIndicatorKey, source.ID, TRIM(source.Practice), TRIM(source.Clinic), TRIM(source.FastMedIndicator), source.EffectiveDate, source.TermedDate, source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UCCFastMedIndicatorKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTUCCFastMedIndicator
      GROUP BY UCCFastMedIndicatorKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTUCCFastMedIndicator');
ELSE
  COMMIT TRANSACTION;
END IF;
