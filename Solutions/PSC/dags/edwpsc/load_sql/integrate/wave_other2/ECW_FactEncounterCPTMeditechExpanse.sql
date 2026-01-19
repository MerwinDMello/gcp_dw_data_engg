
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterCPTMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterCPTMeditechExpanse AS source
ON target.EncounterCPTMTXKey = source.EncounterCPTMTXKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterCPTMTXKey = source.EncounterCPTMTXKey,
 target.RegionKey = source.RegionKey,
 target.EncounterKey = source.EncounterKey,
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTUnits = source.CPTUnits,
 target.CPTOrder = source.CPTOrder,
 target.CPTMod1 = TRIM(source.CPTMod1),
 target.VisitDate = source.VisitDate,
 target.DeleteFlag = source.DeleteFlag,
 target.ChargeCode = TRIM(source.ChargeCode),
 target.SourceLastUpdatedDate = source.SourceLastUpdatedDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.CPTTransNum = source.CPTTransNum
WHEN NOT MATCHED THEN
  INSERT (EncounterCPTMTXKey, RegionKey, EncounterKey, CPTCodeKey, CPTCode, CPTUnits, CPTOrder, CPTMod1, VisitDate, DeleteFlag, ChargeCode, SourceLastUpdatedDate, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, CPTTransNum)
  VALUES (source.EncounterCPTMTXKey, source.RegionKey, source.EncounterKey, source.CPTCodeKey, TRIM(source.CPTCode), source.CPTUnits, source.CPTOrder, TRIM(source.CPTMod1), source.VisitDate, source.DeleteFlag, TRIM(source.ChargeCode), source.SourceLastUpdatedDate, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.CPTTransNum);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterCPTMTXKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterCPTMeditechExpanse
      GROUP BY EncounterCPTMTXKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterCPTMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
