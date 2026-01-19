
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactUserTouch AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactUserTouch AS source
ON target.UserTouchKey = source.UserTouchKey
WHEN MATCHED THEN
  UPDATE SET
  target.UserTouchKey = source.UserTouchKey,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.User34 = TRIM(source.User34),
 target.RegionKey = source.RegionKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterID = source.EncounterID,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.TouchDateKey = source.TouchDateKey,
 target.TouchBeginDateTime = source.TouchBeginDateTime,
 target.TouchEndDateTime = source.TouchEndDateTime,
 target.TouchType = TRIM(source.TouchType),
 target.COID = TRIM(source.COID),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ActionTableName = TRIM(source.ActionTableName),
 target.ActionColumnName = TRIM(source.ActionColumnName),
 target.ActionKey = source.ActionKey,
 target.AdditionalDataType1 = TRIM(source.AdditionalDataType1),
 target.AdditionalDataValue1 = TRIM(source.AdditionalDataValue1),
 target.AdditionalDataType2 = TRIM(source.AdditionalDataType2),
 target.AdditionalDataValue2 = TRIM(source.AdditionalDataValue2),
 target.TimeInMinutes = source.TimeInMinutes,
 target.TimeGapMinutes = source.TimeGapMinutes
WHEN NOT MATCHED THEN
  INSERT (UserTouchKey, SourceSystemCode, User34, RegionKey, EncounterKey, EncounterID, ClaimKey, ClaimNumber, TouchDateKey, TouchBeginDateTime, TouchEndDateTime, TouchType, COID, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ActionTableName, ActionColumnName, ActionKey, AdditionalDataType1, AdditionalDataValue1, AdditionalDataType2, AdditionalDataValue2, TimeInMinutes, TimeGapMinutes)
  VALUES (source.UserTouchKey, TRIM(source.SourceSystemCode), TRIM(source.User34), source.RegionKey, source.EncounterKey, source.EncounterID, source.ClaimKey, source.ClaimNumber, source.TouchDateKey, source.TouchBeginDateTime, source.TouchEndDateTime, TRIM(source.TouchType), TRIM(source.COID), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ActionTableName), TRIM(source.ActionColumnName), source.ActionKey, TRIM(source.AdditionalDataType1), TRIM(source.AdditionalDataValue1), TRIM(source.AdditionalDataType2), TRIM(source.AdditionalDataValue2), source.TimeInMinutes, source.TimeGapMinutes);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UserTouchKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactUserTouch
      GROUP BY UserTouchKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactUserTouch');
ELSE
  COMMIT TRANSACTION;
END IF;
