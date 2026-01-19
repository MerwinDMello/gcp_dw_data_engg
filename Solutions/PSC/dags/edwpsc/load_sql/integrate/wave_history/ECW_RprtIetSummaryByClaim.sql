
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtIetSummaryByClaim AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtIetSummaryByClaim AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.SnapshotDate = TRIM(source.SnapshotDate),
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.CoidLOB = TRIM(source.CoidLOB),
 target.CoidSubLOB = TRIM(source.CoidSubLOB),
 target.COID = TRIM(source.COID),
 target.CoidName = TRIM(source.CoidName),
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.AgeInDays = source.AgeInDays,
 target.ErrorType = TRIM(source.ErrorType),
 target.SubcategoryName = TRIM(source.SubcategoryName),
 target.PlaceOfServiceCode = TRIM(source.PlaceOfServiceCode),
 target.TotalBalance = source.TotalBalance,
 target.IetErrorCount = source.IetErrorCount,
 target.ClaimUnbilledStatusKey = source.ClaimUnbilledStatusKey,
 target.UnbilledStatusCategory = TRIM(source.UnbilledStatusCategory),
 target.UnbilledStatusSubcategory = TRIM(source.UnbilledStatusSubcategory),
 target.ServicingProviderId = source.ServicingProviderId,
 target.ServicingProviderName = TRIM(source.ServicingProviderName),
 target.ServicingProviderNPI = TRIM(source.ServicingProviderNPI),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapshotDate, GroupName, DivisionName, MarketName, CoidLOB, CoidSubLOB, COID, CoidName, ClaimKey, ClaimNumber, AgeInDays, ErrorType, SubcategoryName, PlaceOfServiceCode, TotalBalance, IetErrorCount, ClaimUnbilledStatusKey, UnbilledStatusCategory, UnbilledStatusSubcategory, ServicingProviderId, ServicingProviderName, ServicingProviderNPI, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
  VALUES (TRIM(source.SnapshotDate), TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.CoidLOB), TRIM(source.CoidSubLOB), TRIM(source.COID), TRIM(source.CoidName), source.ClaimKey, source.ClaimNumber, source.AgeInDays, TRIM(source.ErrorType), TRIM(source.SubcategoryName), TRIM(source.PlaceOfServiceCode), source.TotalBalance, source.IetErrorCount, source.ClaimUnbilledStatusKey, TRIM(source.UnbilledStatusCategory), TRIM(source.UnbilledStatusSubcategory), source.ServicingProviderId, TRIM(source.ServicingProviderName), TRIM(source.ServicingProviderNPI), TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtIetSummaryByClaim
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtIetSummaryByClaim');
ELSE
  COMMIT TRANSACTION;
END IF;
