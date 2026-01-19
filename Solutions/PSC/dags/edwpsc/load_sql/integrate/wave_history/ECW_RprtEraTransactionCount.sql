
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtEraTransactionCount AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtEraTransactionCount AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.CreateYear = TRIM(source.CreateYear),
 target.CreateMonth = TRIM(source.CreateMonth),
 target.EcwRegionId = source.EcwRegionId,
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.LOB = TRIM(source.LOB),
 target.COID = TRIM(source.COID),
 target.CoidName = TRIM(source.CoidName),
 target.CoidStartMonth = source.CoidStartMonth,
 target.CoidTermMonth = source.CoidTermMonth,
 target.PracticeFederalTaxId = TRIM(source.PracticeFederalTaxId),
 target.GroupNPI = TRIM(source.GroupNPI),
 target.InsuranceGroupName = TRIM(source.InsuranceGroupName),
 target.PayorName = TRIM(source.PayorName),
 target.RenderingProvider = TRIM(source.RenderingProvider),
 target.RenderingProviderStartMonth = source.RenderingProviderStartMonth,
 target.RenderingProviderTermMonth = source.RenderingProviderTermMonth,
 target.ClaimCount = source.ClaimCount,
 target.EraPaid = source.EraPaid,
 target.ManualPaid = source.ManualPaid,
 target.EraCount = source.EraCount,
 target.ManualCount = source.ManualCount,
 target.FifthThirdPaid = source.FifthThirdPaid,
 target.NonFifthThirdPaid = source.NonFifthThirdPaid,
 target.FifthThirdCount = source.FifthThirdCount,
 target.NonFifthThirdCount = source.NonFifthThirdCount,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, CreateYear, CreateMonth, EcwRegionId, GroupName, DivisionName, MarketName, LOB, COID, CoidName, CoidStartMonth, CoidTermMonth, PracticeFederalTaxId, GroupNPI, InsuranceGroupName, PayorName, RenderingProvider, RenderingProviderStartMonth, RenderingProviderTermMonth, ClaimCount, EraPaid, ManualPaid, EraCount, ManualCount, FifthThirdPaid, NonFifthThirdPaid, FifthThirdCount, NonFifthThirdCount, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, TRIM(source.CreateYear), TRIM(source.CreateMonth), source.EcwRegionId, TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), TRIM(source.LOB), TRIM(source.COID), TRIM(source.CoidName), source.CoidStartMonth, source.CoidTermMonth, TRIM(source.PracticeFederalTaxId), TRIM(source.GroupNPI), TRIM(source.InsuranceGroupName), TRIM(source.PayorName), TRIM(source.RenderingProvider), source.RenderingProviderStartMonth, source.RenderingProviderTermMonth, source.ClaimCount, source.EraPaid, source.ManualPaid, source.EraCount, source.ManualCount, source.FifthThirdPaid, source.NonFifthThirdPaid, source.FifthThirdCount, source.NonFifthThirdCount, TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtEraTransactionCount
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtEraTransactionCount');
ELSE
  COMMIT TRANSACTION;
END IF;
