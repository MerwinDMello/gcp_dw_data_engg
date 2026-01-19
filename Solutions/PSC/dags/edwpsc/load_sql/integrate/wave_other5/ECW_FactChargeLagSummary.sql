
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactChargeLagSummary AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactChargeLagSummary AS source
ON target.ChargeLagSummaryKey = source.ChargeLagSummaryKey
WHEN MATCHED THEN
  UPDATE SET
  target.ChargeLagSummaryKey = source.ChargeLagSummaryKey,
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.COIDName = TRIM(source.COIDName),
 target.Coid = TRIM(source.Coid),
 target.LOBCode = TRIM(source.LOBCode),
 target.SubLOBCode = TRIM(source.SubLOBCode),
 target.SubLOBName = TRIM(source.SubLOBName),
 target.POSCode = TRIM(source.POSCode),
 target.GLDeptNum = TRIM(source.GLDeptNum),
 target.FromServicePEDate = source.FromServicePEDate,
 target.StartEntryDate = source.StartEntryDate,
 target.GLPostingPEDate = source.GLPostingPEDate,
 target.ChargeLagStartDate = source.ChargeLagStartDate,
 target.ChargeLagStartPEDate = source.ChargeLagStartPEDate,
 target.ChargeLagBucket = TRIM(source.ChargeLagBucket),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.SourceSystemDesc = TRIM(source.SourceSystemDesc),
 target.FLevel = TRIM(source.FLevel),
 target.ProviderName = TRIM(source.ProviderName),
 target.LargePractice = TRIM(source.LargePractice),
 target.ProviderSpecialtyCategory = TRIM(source.ProviderSpecialtyCategory),
 target.ProviderSpecialtyType = TRIM(source.ProviderSpecialtyType),
 target.ProviderSpecialty = TRIM(source.ProviderSpecialty),
 target.LagDayTotalDen = source.LagDayTotalDen,
 target.LagDayTotalNum = source.LagDayTotalNum,
 target.Lag02Den = source.Lag02Den,
 target.Lag35Den = source.Lag35Den,
 target.LagGt5Den = source.LagGt5Den,
 target.LagLt0Den = source.LagLt0Den,
 target.LagUnknownDen = source.LagUnknownDen,
 target.Lag02Num = source.Lag02Num,
 target.Lag35Num = source.Lag35Num,
 target.LagGt5Num = source.LagGt5Num,
 target.LagLt0Num = source.LagLt0Num,
 target.LagUnknownNum = source.LagUnknownNum,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (ChargeLagSummaryKey, GroupName, DivisionName, MarketName, COIDName, Coid, LOBCode, SubLOBCode, SubLOBName, POSCode, GLDeptNum, FromServicePEDate, StartEntryDate, GLPostingPEDate, ChargeLagStartDate, ChargeLagStartPEDate, ChargeLagBucket, SourceSystemCode, SourceSystemDesc, FLevel, ProviderName, LargePractice, ProviderSpecialtyCategory, ProviderSpecialtyType, ProviderSpecialty, LagDayTotalDen, LagDayTotalNum, Lag02Den, Lag35Den, LagGt5Den, LagLt0Den, LagUnknownDen, Lag02Num, Lag35Num, LagGt5Num, LagLt0Num, LagUnknownNum, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.ChargeLagSummaryKey, TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.COIDName), TRIM(source.Coid), TRIM(source.LOBCode), TRIM(source.SubLOBCode), TRIM(source.SubLOBName), TRIM(source.POSCode), TRIM(source.GLDeptNum), source.FromServicePEDate, source.StartEntryDate, source.GLPostingPEDate, source.ChargeLagStartDate, source.ChargeLagStartPEDate, TRIM(source.ChargeLagBucket), TRIM(source.SourceSystemCode), TRIM(source.SourceSystemDesc), TRIM(source.FLevel), TRIM(source.ProviderName), TRIM(source.LargePractice), TRIM(source.ProviderSpecialtyCategory), TRIM(source.ProviderSpecialtyType), TRIM(source.ProviderSpecialty), source.LagDayTotalDen, source.LagDayTotalNum, source.Lag02Den, source.Lag35Den, source.LagGt5Den, source.LagLt0Den, source.LagUnknownDen, source.Lag02Num, source.Lag35Num, source.LagGt5Num, source.LagLt0Num, source.LagUnknownNum, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ChargeLagSummaryKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactChargeLagSummary
      GROUP BY ChargeLagSummaryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactChargeLagSummary');
ELSE
  COMMIT TRANSACTION;
END IF;
