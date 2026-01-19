
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMeditechExpanseNotes AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterMeditechExpanseNotes AS source
ON target.EncounterMTXNotesKey = source.EncounterMTXNotesKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterMTXNotesKey = source.EncounterMTXNotesKey,
 target.SourceID = TRIM(source.SourceID),
 target.VisitID = TRIM(source.VisitID),
 target.CodingAccountVisitDateTimeID = source.CodingAccountVisitDateTimeID,
 target.CodingHistoryUrnID = source.CodingHistoryUrnID,
 target.RowUpdateDateTime = source.RowUpdateDateTime,
 target.CodingHistoryDateTime = source.CodingHistoryDateTime,
 target.CodingHistoryType = TRIM(source.CodingHistoryType),
 target.CodingHistoryUser_UnvUserID = TRIM(source.CodingHistoryUser_UnvUserID),
 target.CodingHistoryOldValue = TRIM(source.CodingHistoryOldValue),
 target.CodingHistoryNewValue = TRIM(source.CodingHistoryNewValue),
 target.CodingHistoryInformationOnly = TRIM(source.CodingHistoryInformationOnly),
 target.CodingHistoryAdditionalData = TRIM(source.CodingHistoryAdditionalData),
 target.RegionId = source.RegionId,
 target.AccountNumber = TRIM(source.AccountNumber),
 target.TextSeqID = source.TextSeqID,
 target.TextID = source.TextID,
 target.TextLine = TRIM(source.TextLine),
 target.TextTimeStamp = TRIM(source.TextTimeStamp),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = source.InsertedBy,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = source.ModifiedBy,
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterMTXNotesKey, SourceID, VisitID, CodingAccountVisitDateTimeID, CodingHistoryUrnID, RowUpdateDateTime, CodingHistoryDateTime, CodingHistoryType, CodingHistoryUser_UnvUserID, CodingHistoryOldValue, CodingHistoryNewValue, CodingHistoryInformationOnly, CodingHistoryAdditionalData, RegionId, AccountNumber, TextSeqID, TextID, TextLine, TextTimeStamp, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterMTXNotesKey, TRIM(source.SourceID), TRIM(source.VisitID), source.CodingAccountVisitDateTimeID, source.CodingHistoryUrnID, source.RowUpdateDateTime, source.CodingHistoryDateTime, TRIM(source.CodingHistoryType), TRIM(source.CodingHistoryUser_UnvUserID), TRIM(source.CodingHistoryOldValue), TRIM(source.CodingHistoryNewValue), TRIM(source.CodingHistoryInformationOnly), TRIM(source.CodingHistoryAdditionalData), source.RegionId, TRIM(source.AccountNumber), source.TextSeqID, source.TextID, TRIM(source.TextLine), TRIM(source.TextTimeStamp), TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, source.InsertedBy, source.InsertedDTM, source.ModifiedBy, source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterMTXNotesKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMeditechExpanseNotes
      GROUP BY EncounterMTXNotesKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterMeditechExpanseNotes');
ELSE
  COMMIT TRANSACTION;
END IF;
