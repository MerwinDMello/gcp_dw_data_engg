
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtPayerResponseTrending AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtPayerResponseTrending AS source
ON target.PayerResponseTrendingKey = source.PayerResponseTrendingKey
WHEN MATCHED THEN
  UPDATE SET
  target.PayerResponseTrendingKey = source.PayerResponseTrendingKey,
 target.SnapShotQuarter = TRIM(source.SnapShotQuarter),
 target.SnapShotDate = source.SnapShotDate,
 target.SnapShotYear = source.SnapShotYear,
 target.DOSQuarter = TRIM(source.DOSQuarter),
 target.DOSMonth = source.DOSMonth,
 target.DOSYear = source.DOSYear,
 target.Coid = TRIM(source.Coid),
 target.POSKey = source.POSKey,
 target.ServicingProviderLastName = TRIM(source.ServicingProviderLastName),
 target.ServicingProviderFirstName = TRIM(source.ServicingProviderFirstName),
 target.ServicingProviderNPI = TRIM(source.ServicingProviderNPI),
 target.GLDepartment = TRIM(source.GLDepartment),
 target.SpecialtyCategory = TRIM(source.SpecialtyCategory),
 target.SpecialtyName = TRIM(source.SpecialtyName),
 target.SpecialtyType = TRIM(source.SpecialtyType),
 target.Iplan = TRIM(source.Iplan),
 target.MajorPayor = TRIM(source.MajorPayor),
 target.FinancialClassName = TRIM(source.FinancialClassName),
 target.FinancialClassNumber = source.FinancialClassNumber,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTName = TRIM(source.CPTName),
 target.FirstDenialCategory = TRIM(source.FirstDenialCategory),
 target.InitialPayerResponseCategory = TRIM(source.InitialPayerResponseCategory),
 target.AdjCode = TRIM(source.AdjCode),
 target.AdjName = TRIM(source.AdjName),
 target.SourceSystem = TRIM(source.SourceSystem),
 target.Charges = source.Charges,
 target.NewDenialsAmt = source.NewDenialsAmt,
 target.NewPaymentVariancesAmt = source.NewPaymentVariancesAmt,
 target.NewClaimRejectionsAmt = source.NewClaimRejectionsAmt,
 target.NewPayerReponseAmt = source.NewPayerReponseAmt,
 target.FinalDenialsAmt = source.FinalDenialsAmt,
 target.FinalPaymentVariancesAmt = source.FinalPaymentVariancesAmt,
 target.FinalClaimRejectionsAmt = source.FinalClaimRejectionsAmt,
 target.ProviderEnrollmentAmt = source.ProviderEnrollmentAmt,
 target.FinalPayerResponseAmt = source.FinalPayerResponseAmt,
 target.DWLastUpdatedDate = source.DWLastUpdatedDate,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (PayerResponseTrendingKey, SnapShotQuarter, SnapShotDate, SnapShotYear, DOSQuarter, DOSMonth, DOSYear, Coid, POSKey, ServicingProviderLastName, ServicingProviderFirstName, ServicingProviderNPI, GLDepartment, SpecialtyCategory, SpecialtyName, SpecialtyType, Iplan, MajorPayor, FinancialClassName, FinancialClassNumber, CPTCode, CPTName, FirstDenialCategory, InitialPayerResponseCategory, AdjCode, AdjName, SourceSystem, Charges, NewDenialsAmt, NewPaymentVariancesAmt, NewClaimRejectionsAmt, NewPayerReponseAmt, FinalDenialsAmt, FinalPaymentVariancesAmt, FinalClaimRejectionsAmt, ProviderEnrollmentAmt, FinalPayerResponseAmt, DWLastUpdatedDate, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.PayerResponseTrendingKey, TRIM(source.SnapShotQuarter), source.SnapShotDate, source.SnapShotYear, TRIM(source.DOSQuarter), source.DOSMonth, source.DOSYear, TRIM(source.Coid), source.POSKey, TRIM(source.ServicingProviderLastName), TRIM(source.ServicingProviderFirstName), TRIM(source.ServicingProviderNPI), TRIM(source.GLDepartment), TRIM(source.SpecialtyCategory), TRIM(source.SpecialtyName), TRIM(source.SpecialtyType), TRIM(source.Iplan), TRIM(source.MajorPayor), TRIM(source.FinancialClassName), source.FinancialClassNumber, TRIM(source.CPTCode), TRIM(source.CPTName), TRIM(source.FirstDenialCategory), TRIM(source.InitialPayerResponseCategory), TRIM(source.AdjCode), TRIM(source.AdjName), TRIM(source.SourceSystem), source.Charges, source.NewDenialsAmt, source.NewPaymentVariancesAmt, source.NewClaimRejectionsAmt, source.NewPayerReponseAmt, source.FinalDenialsAmt, source.FinalPaymentVariancesAmt, source.FinalClaimRejectionsAmt, source.ProviderEnrollmentAmt, source.FinalPayerResponseAmt, source.DWLastUpdatedDate, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PayerResponseTrendingKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtPayerResponseTrending
      GROUP BY PayerResponseTrendingKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtPayerResponseTrending');
ELSE
  COMMIT TRANSACTION;
END IF;
