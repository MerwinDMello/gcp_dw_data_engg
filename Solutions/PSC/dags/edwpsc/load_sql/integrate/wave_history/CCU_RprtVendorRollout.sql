
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorRollout AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtVendorRollout AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.CCUVendorRolloutKey = source.CCUVendorRolloutKey,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.CoidName = TRIM(source.CoidName),
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.CoidSpecialty = TRIM(source.CoidSpecialty),
 target.CoidCentralizedStatus = TRIM(source.CoidCentralizedStatus),
 target.COIDStartDateKey = source.COIDStartDateKey,
 target.COIDTermDateKey = source.COIDTermDateKey,
 target.CoidLOB = TRIM(source.CoidLOB),
 target.CoidSubLOB = TRIM(source.CoidSubLOB),
 target.CoidSystem = TRIM(source.CoidSystem),
 target.PPMSFlag = TRIM(source.PPMSFlag),
 target.CoidLessThan120daysActive = TRIM(source.CoidLessThan120daysActive),
 target.CorrectCptCount = source.CorrectCptCount,
 target.TotalCpts = source.TotalCpts,
 target.PercentageCoidCorrect = source.PercentageCoidCorrect,
 target.CorrectRate = source.CorrectRate,
 target.DenialCount = source.DenialCount,
 target.TotalQualityCount = source.TotalQualityCount,
 target.PercentageCoidInDenied = source.PercentageCoidInDenied,
 target.DenialRate = source.DenialRate,
 target.TicketPerMo = source.TicketPerMo,
 target.TotalPRPTickets = source.TotalPRPTickets,
 target.PrpRateLow = source.PrpRateLow,
 target.OutcomeRecommendation = TRIM(source.OutcomeRecommendation),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedyBy = TRIM(source.ModifiedyBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.CoidExclusionFlag = TRIM(source.CoidExclusionFlag),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (CCUVendorRolloutKey, SnapShotDate, Coid, CoidName, GroupName, DivisionName, MarketName, CoidSpecialty, CoidCentralizedStatus, COIDStartDateKey, COIDTermDateKey, CoidLOB, CoidSubLOB, CoidSystem, PPMSFlag, CoidLessThan120daysActive, CorrectCptCount, TotalCpts, PercentageCoidCorrect, CorrectRate, DenialCount, TotalQualityCount, PercentageCoidInDenied, DenialRate, TicketPerMo, TotalPRPTickets, PrpRateLow, OutcomeRecommendation, InsertedBy, InsertedDTM, ModifiedyBy, ModifiedDTM, CoidExclusionFlag, DWLastUpdateDateTime)
  VALUES (source.CCUVendorRolloutKey, source.SnapShotDate, TRIM(source.Coid), TRIM(source.CoidName), TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.CoidSpecialty), TRIM(source.CoidCentralizedStatus), source.COIDStartDateKey, source.COIDTermDateKey, TRIM(source.CoidLOB), TRIM(source.CoidSubLOB), TRIM(source.CoidSystem), TRIM(source.PPMSFlag), TRIM(source.CoidLessThan120daysActive), source.CorrectCptCount, source.TotalCpts, source.PercentageCoidCorrect, source.CorrectRate, source.DenialCount, source.TotalQualityCount, source.PercentageCoidInDenied, source.DenialRate, source.TicketPerMo, source.TotalPRPTickets, source.PrpRateLow, TRIM(source.OutcomeRecommendation), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedyBy), source.ModifiedDTM, TRIM(source.CoidExclusionFlag), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorRollout
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorRollout');
ELSE
  COMMIT TRANSACTION;
END IF;
