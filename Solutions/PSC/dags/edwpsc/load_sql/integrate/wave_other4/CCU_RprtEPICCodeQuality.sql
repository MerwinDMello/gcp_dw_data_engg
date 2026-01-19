
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtEPICCodeQuality AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtEPICCodeQuality AS source
ON target.ClaimKey = source.ClaimKey AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.CodeQualityKey = source.CodeQualityKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.COID = TRIM(source.COID),
 target.ServiceDateKey = source.ServiceDateKey,
 target.SvcProviderSpecialty = TRIM(source.SvcProviderSpecialty),
 target.FirstInsBillDateWkOfMonth = source.FirstInsBillDateWkOfMonth,
 target.FirstDenialCategories = TRIM(source.FirstDenialCategories),
 target.PreBillEditCategories = TRIM(source.PreBillEditCategories),
 target.QUALITYCATEGORY = TRIM(source.QUALITYCATEGORY),
 target.Coder = TRIM(source.Coder),
 target.Coder34Id = TRIM(source.Coder34Id),
 target.CoderType = TRIM(source.CoderType),
 target.ClaimCount = source.ClaimCount,
 target.PracticeId = TRIM(source.PracticeId),
 target.PatientMRN = TRIM(source.PatientMRN),
 target.PatientInternalId = source.PatientInternalId,
 target.VisitNumber = source.VisitNumber,
 target.TransactionNumber_combined = TRIM(source.TransactionNumber_combined),
 target.FirstInsuranceBillDate = source.FirstInsuranceBillDate,
 target.MinFirstDenialERADate = source.MinFirstDenialERADate,
 target.CoderActionDate = source.CoderActionDate,
 target.InvoiceNumber_Combined = TRIM(source.InvoiceNumber_Combined),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.HashNoMatch = source.HashNoMatch,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (CodeQualityKey, ClaimKey, ClaimNumber, RegionKey, COID, ServiceDateKey, SvcProviderSpecialty, FirstInsBillDateWkOfMonth, FirstDenialCategories, PreBillEditCategories, QUALITYCATEGORY, Coder, Coder34Id, CoderType, ClaimCount, PracticeId, PatientMRN, PatientInternalId, VisitNumber, TransactionNumber_combined, FirstInsuranceBillDate, MinFirstDenialERADate, CoderActionDate, InvoiceNumber_Combined, SourceSystemCode, DWLastUpdateDateTime, HashNoMatch, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.CodeQualityKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.COID), source.ServiceDateKey, TRIM(source.SvcProviderSpecialty), source.FirstInsBillDateWkOfMonth, TRIM(source.FirstDenialCategories), TRIM(source.PreBillEditCategories), TRIM(source.QUALITYCATEGORY), TRIM(source.Coder), TRIM(source.Coder34Id), TRIM(source.CoderType), source.ClaimCount, TRIM(source.PracticeId), TRIM(source.PatientMRN), source.PatientInternalId, source.VisitNumber, TRIM(source.TransactionNumber_combined), source.FirstInsuranceBillDate, source.MinFirstDenialERADate, source.CoderActionDate, TRIM(source.InvoiceNumber_Combined), TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, source.HashNoMatch, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimKey, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtEPICCodeQuality
      GROUP BY ClaimKey, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtEPICCodeQuality');
ELSE
  COMMIT TRANSACTION;
END IF;
