
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtGovernedDenialsTrending AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtGovernedDenialsTrending AS source
ON target.GovernedDenialsTrendingKey = source.GovernedDenialsTrendingKey
WHEN MATCHED THEN
  UPDATE SET
  target.GovernedDenialsTrendingKey = source.GovernedDenialsTrendingKey,
 target.SnapShotQuarter = TRIM(source.SnapShotQuarter),
 target.SnapShotDate = source.SnapShotDate,
 target.SnapShotYear = source.SnapShotYear,
 target.DOSQuarter = TRIM(source.DOSQuarter),
 target.DOSMonth = source.DOSMonth,
 target.DOSYear = source.DOSYear,
 target.Coid = TRIM(source.Coid),
 target.SameStoreFlag = TRIM(source.SameStoreFlag),
 target.ServicingProviderLastName = TRIM(source.ServicingProviderLastName),
 target.ServicingProviderFirstName = TRIM(source.ServicingProviderFirstName),
 target.ServicingProviderNPI = TRIM(source.ServicingProviderNPI),
 target.SpecialtyCategory = TRIM(source.SpecialtyCategory),
 target.SpecialtyName = TRIM(source.SpecialtyName),
 target.SpecialtyType = TRIM(source.SpecialtyType),
 target.Iplan = TRIM(source.Iplan),
 target.MajorPayor = TRIM(source.MajorPayor),
 target.FinancialClassName = TRIM(source.FinancialClassName),
 target.FinancialClassNumber = source.FinancialClassNumber,
 target.FirstDenialCategory = TRIM(source.FirstDenialCategory),
 target.AdjCode = TRIM(source.AdjCode),
 target.AdjName = TRIM(source.AdjName),
 target.NonParFlag = TRIM(source.NonParFlag),
 target.AdjCategory = TRIM(source.AdjCategory),
 target.SourceSystem = TRIM(source.SourceSystem),
 target.InitialDeniedCharges = source.InitialDeniedCharges,
 target.Charges = source.Charges,
 target.FinalDenials = source.FinalDenials,
 target.NewDenialsAmt = source.NewDenialsAmt,
 target.NewPaymentVariancesAmt = source.NewPaymentVariancesAmt,
 target.NewClaimRejectionsAmt = source.NewClaimRejectionsAmt,
 target.InitialPayerResponseCategory = TRIM(source.InitialPayerResponseCategory),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (GovernedDenialsTrendingKey, SnapShotQuarter, SnapShotDate, SnapShotYear, DOSQuarter, DOSMonth, DOSYear, Coid, SameStoreFlag, ServicingProviderLastName, ServicingProviderFirstName, ServicingProviderNPI, SpecialtyCategory, SpecialtyName, SpecialtyType, Iplan, MajorPayor, FinancialClassName, FinancialClassNumber, FirstDenialCategory, AdjCode, AdjName, NonParFlag, AdjCategory, SourceSystem, InitialDeniedCharges, Charges, FinalDenials, NewDenialsAmt, NewPaymentVariancesAmt, NewClaimRejectionsAmt, InitialPayerResponseCategory, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.GovernedDenialsTrendingKey, TRIM(source.SnapShotQuarter), source.SnapShotDate, source.SnapShotYear, TRIM(source.DOSQuarter), source.DOSMonth, source.DOSYear, TRIM(source.Coid), TRIM(source.SameStoreFlag), TRIM(source.ServicingProviderLastName), TRIM(source.ServicingProviderFirstName), TRIM(source.ServicingProviderNPI), TRIM(source.SpecialtyCategory), TRIM(source.SpecialtyName), TRIM(source.SpecialtyType), TRIM(source.Iplan), TRIM(source.MajorPayor), TRIM(source.FinancialClassName), source.FinancialClassNumber, TRIM(source.FirstDenialCategory), TRIM(source.AdjCode), TRIM(source.AdjName), TRIM(source.NonParFlag), TRIM(source.AdjCategory), TRIM(source.SourceSystem), source.InitialDeniedCharges, source.Charges, source.FinalDenials, source.NewDenialsAmt, source.NewPaymentVariancesAmt, source.NewClaimRejectionsAmt, TRIM(source.InitialPayerResponseCategory), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT GovernedDenialsTrendingKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtGovernedDenialsTrending
      GROUP BY GovernedDenialsTrendingKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtGovernedDenialsTrending');
ELSE
  COMMIT TRANSACTION;
END IF;
