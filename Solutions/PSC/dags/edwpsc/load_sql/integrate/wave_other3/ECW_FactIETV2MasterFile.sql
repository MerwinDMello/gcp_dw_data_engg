
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2MasterFile AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactIETV2MasterFile AS source
ON target.IETV2MasterFileKey = source.IETV2MasterFileKey
WHEN MATCHED THEN
  UPDATE SET
  target.FieldMessage = TRIM(source.FieldMessage),
 target.Category = TRIM(source.Category),
 target.SubcategoryId = TRIM(source.SubcategoryId),
 target.ReportType = TRIM(source.ReportType),
 target.IsActive = source.IsActive,
 target.NotesFieldName = TRIM(source.NotesFieldName),
 target.IsNotesActive = source.IsNotesActive,
 target.UserFriendlyMessage = TRIM(source.UserFriendlyMessage),
 target.IsUserFriendlyMessageActive = source.IsUserFriendlyMessageActive,
 target.Notes = TRIM(source.Notes),
 target.SubcategoryOriginId = TRIM(source.SubcategoryOriginId),
 target.SubcategoryDescription = TRIM(source.SubcategoryDescription),
 target.SubcategoryActive = source.SubcategoryActive,
 target.Department = TRIM(source.Department),
 target.IsCreateClaimQuery = source.IsCreateClaimQuery,
 target.SubcategoryType = TRIM(source.SubcategoryType),
 target.ClaimErrorTypeName = TRIM(source.ClaimErrorTypeName),
 target.ClaimErrorTypeDescription = TRIM(source.ClaimErrorTypeDescription),
 target.CategoryName = TRIM(source.CategoryName),
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.lastmodifiedActiveFlagToYes = source.lastmodifiedActiveFlagToYes,
 target.FieldName = TRIM(source.FieldName),
 target.IETV2MasterFileKey = source.IETV2MasterFileKey
WHEN NOT MATCHED THEN
  INSERT (FieldMessage, Category, SubcategoryId, ReportType, IsActive, NotesFieldName, IsNotesActive, UserFriendlyMessage, IsUserFriendlyMessageActive, Notes, SubcategoryOriginId, SubcategoryDescription, SubcategoryActive, Department, IsCreateClaimQuery, SubcategoryType, ClaimErrorTypeName, ClaimErrorTypeDescription, CategoryName, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, lastmodifiedActiveFlagToYes, FieldName, IETV2MasterFileKey)
  VALUES (TRIM(source.FieldMessage), TRIM(source.Category), TRIM(source.SubcategoryId), TRIM(source.ReportType), source.IsActive, TRIM(source.NotesFieldName), source.IsNotesActive, TRIM(source.UserFriendlyMessage), source.IsUserFriendlyMessageActive, TRIM(source.Notes), TRIM(source.SubcategoryOriginId), TRIM(source.SubcategoryDescription), source.SubcategoryActive, TRIM(source.Department), source.IsCreateClaimQuery, TRIM(source.SubcategoryType), TRIM(source.ClaimErrorTypeName), TRIM(source.ClaimErrorTypeDescription), TRIM(source.CategoryName), source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.lastmodifiedActiveFlagToYes, TRIM(source.FieldName), source.IETV2MasterFileKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IETV2MasterFileKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2MasterFile
      GROUP BY IETV2MasterFileKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2MasterFile');
ELSE
  COMMIT TRANSACTION;
END IF;
