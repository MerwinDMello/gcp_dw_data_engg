
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_RefVisitType AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_RefVisitType AS source
ON target.VisitTypeKey = source.VisitTypeKey
WHEN MATCHED THEN
  UPDATE SET
  target.VisitTypeKey = source.VisitTypeKey,
 target.VisitTypeName = TRIM(source.VisitTypeName),
 target.VisitTypeAbbr = TRIM(source.VisitTypeAbbr),
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
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag,
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (VisitTypeKey, VisitTypeName, VisitTypeAbbr, VisitTypeDescription, VisitTypeRequiredClaim, VisitTypeRequiredCopay, VisitTypePregnancyVisit, VisitTypeActiveFlag, VisitTypeOrthoVisit, VisitTypeObgynVisit, VisitTypeIsVisit, VisitTypeWebVisit, VisitTypePhysicalTherapyVisit, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag, RegionKey)
  VALUES (source.VisitTypeKey, TRIM(source.VisitTypeName), TRIM(source.VisitTypeAbbr), TRIM(source.VisitTypeDescription), source.VisitTypeRequiredClaim, source.VisitTypeRequiredCopay, source.VisitTypePregnancyVisit, source.VisitTypeActiveFlag, source.VisitTypeOrthoVisit, source.VisitTypeObgynVisit, source.VisitTypeIsVisit, source.VisitTypeWebVisit, source.VisitTypePhysicalTherapyVisit, TRIM(source.SourcePrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag, source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT VisitTypeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_RefVisitType
      GROUP BY VisitTypeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_RefVisitType');
ELSE
  COMMIT TRANSACTION;
END IF;
