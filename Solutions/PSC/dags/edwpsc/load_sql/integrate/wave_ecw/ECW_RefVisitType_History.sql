
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefVisitType_History AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefVisitType_History AS source
ON target.VisitTypeKey = source.VisitTypeKey AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.VisitTypeKey = source.VisitTypeKey,
 target.VisitTypeName = TRIM(source.VisitTypeName),
 target.VisitTypeDescription = TRIM(source.VisitTypeDescription),
 target.VisitTypeRequiredClaim = source.VisitTypeRequiredClaim,
 target.VisitTypeRequiredCopay = source.VisitTypeRequiredCopay,
 target.VisitTypePregnancyVisit = source.VisitTypePregnancyVisit,
 target.VisitTypeActiveFlag = source.VisitTypeActiveFlag,
 target.VisitTypeOrthoVisit = source.VisitTypeOrthoVisit,
 target.VisitTypeObgynVisit = source.VisitTypeObgynVisit,
 target.VisitTypeIsVisit = source.VisitTypeIsVisit,
 target.VisitTypeWebVisit = source.VisitTypeWebVisit,
 target.VisitTypePhysicalTherapyVisit = source.VisitTypePhysicalTherapyVisit,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime
WHEN NOT MATCHED THEN
  INSERT (VisitTypeKey, VisitTypeName, VisitTypeDescription, VisitTypeRequiredClaim, VisitTypeRequiredCopay, VisitTypePregnancyVisit, VisitTypeActiveFlag, VisitTypeOrthoVisit, VisitTypeObgynVisit, VisitTypeIsVisit, VisitTypeWebVisit, VisitTypePhysicalTherapyVisit, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, SysStartTime, SysEndTime)
  VALUES (source.VisitTypeKey, TRIM(source.VisitTypeName), TRIM(source.VisitTypeDescription), source.VisitTypeRequiredClaim, source.VisitTypeRequiredCopay, source.VisitTypePregnancyVisit, source.VisitTypeActiveFlag, source.VisitTypeOrthoVisit, source.VisitTypeObgynVisit, source.VisitTypeIsVisit, source.VisitTypeWebVisit, source.VisitTypePhysicalTherapyVisit, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.SysStartTime, source.SysEndTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT VisitTypeKey, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefVisitType_History
      GROUP BY VisitTypeKey, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefVisitType_History');
ELSE
  COMMIT TRANSACTION;
END IF;
