
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtCoidProvider AS source
ON target.SnapShotDate = source.SnapShotDate AND target.CCUCoidProviderHistoryKey = source.CCUCoidProviderHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUCoidProviderHistoryKey = source.CCUCoidProviderHistoryKey,
 target.MORSnapShotDateKey = source.MORSnapShotDateKey,
 target.Coid = TRIM(source.Coid),
 target.CoidStartDateKey = source.CoidStartDateKey,
 target.CoidTermDateKey = source.CoidTermDateKey,
 target.CoidSystem = TRIM(source.CoidSystem),
 target.CoidConsolidationDate = TRIM(source.CoidConsolidationDate),
 target.CCUDiscontinuedDate = TRIM(source.CCUDiscontinuedDate),
 target.CoidCentralizedStatus = TRIM(source.CoidCentralizedStatus),
 target.CoidStatus = TRIM(source.CoidStatus),
 target.CoidLevelOfCentralization = TRIM(source.CoidLevelOfCentralization),
 target.GmeCoid = TRIM(source.GmeCoid),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderName = TRIM(source.ProviderName),
 target.ProviderSpecialty = TRIM(source.ProviderSpecialty),
 target.ProviderStartDateKey = source.ProviderStartDateKey,
 target.ProviderTermDateKey = source.ProviderTermDateKey,
 target.ProviderStatus = TRIM(source.ProviderStatus),
 target.ProviderCentralizedStatus = TRIM(source.ProviderCentralizedStatus),
 target.ProviderGroupAssignment = TRIM(source.ProviderGroupAssignment),
 target.FTE = source.FTE,
 target.ProviderCountActive = source.ProviderCountActive,
 target.ProviderCountTermed = source.ProviderCountTermed,
 target.ProviderCountArchiveClosed = source.ProviderCountArchiveClosed,
 target.CoidCount = source.CoidCount,
 target.LoadDate = source.LoadDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.CoidExclusionFlag = TRIM(source.CoidExclusionFlag),
 target.SnapShotDate = source.SnapShotDate
WHEN NOT MATCHED THEN
  INSERT (CCUCoidProviderHistoryKey, MORSnapShotDateKey, Coid, CoidStartDateKey, CoidTermDateKey, CoidSystem, CoidConsolidationDate, CCUDiscontinuedDate, CoidCentralizedStatus, CoidStatus, CoidLevelOfCentralization, GmeCoid, ProviderNPI, ProviderName, ProviderSpecialty, ProviderStartDateKey, ProviderTermDateKey, ProviderStatus, ProviderCentralizedStatus, ProviderGroupAssignment, FTE, ProviderCountActive, ProviderCountTermed, ProviderCountArchiveClosed, CoidCount, LoadDate, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, CoidExclusionFlag, SnapShotDate)
  VALUES (source.CCUCoidProviderHistoryKey, source.MORSnapShotDateKey, TRIM(source.Coid), source.CoidStartDateKey, source.CoidTermDateKey, TRIM(source.CoidSystem), TRIM(source.CoidConsolidationDate), TRIM(source.CCUDiscontinuedDate), TRIM(source.CoidCentralizedStatus), TRIM(source.CoidStatus), TRIM(source.CoidLevelOfCentralization), TRIM(source.GmeCoid), TRIM(source.ProviderNPI), TRIM(source.ProviderName), TRIM(source.ProviderSpecialty), source.ProviderStartDateKey, source.ProviderTermDateKey, TRIM(source.ProviderStatus), TRIM(source.ProviderCentralizedStatus), TRIM(source.ProviderGroupAssignment), source.FTE, source.ProviderCountActive, source.ProviderCountTermed, source.ProviderCountArchiveClosed, source.CoidCount, source.LoadDate, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.CoidExclusionFlag), source.SnapShotDate);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, CCUCoidProviderHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidProvider
      GROUP BY SnapShotDate, CCUCoidProviderHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
