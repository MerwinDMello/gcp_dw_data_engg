
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RprtCCUClaimSummaryDX AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RprtCCUClaimSummaryDX AS source
ON target.CCUClaimSummaryDXKey = source.CCUClaimSummaryDXKey
WHEN MATCHED THEN
  UPDATE SET
  target.SystemName = TRIM(source.SystemName),
 target.DosProviderName = TRIM(source.DosProviderName),
 target.ClaimNumber = source.ClaimNumber,
 target.EncounterID = source.EncounterID,
 target.EncounterCount = source.EncounterCount,
 target.BillingArea = TRIM(source.BillingArea),
 target.CoidSpecialty = TRIM(source.CoidSpecialty),
 target.COID = TRIM(source.COID),
 target.ClaimCoid = TRIM(source.ClaimCoid),
 target.RegionKey = source.RegionKey,
 target.EncounterDate = source.EncounterDate,
 target.EMRDXsVsClaimDXs = TRIM(source.EMRDXsVsClaimDXs),
 target.DxCodeChange = TRIM(source.DxCodeChange),
 target.EncounterDXCode = TRIM(source.EncounterDXCode),
 target.ClaimDXCode = TRIM(source.ClaimDXCode),
 target.LastChangedBy34Dept = TRIM(source.LastChangedBy34Dept),
 target.BillingNotes = TRIM(source.BillingNotes),
 target.PatientAccountNumber = source.PatientAccountNumber,
 target.FinancialNumber = TRIM(source.FinancialNumber),
 target.PatientName = TRIM(source.PatientName),
 target.EncounterDateYYYYMM = TRIM(source.EncounterDateYYYYMM),
 target.LastTouchedDate_YYYYMM = TRIM(source.LastTouchedDate_YYYYMM),
 target.EncounterCoidLob = TRIM(source.EncounterCoidLob),
 target.EncounterCoidSubLob = TRIM(source.EncounterCoidSubLob),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.EncounterType = TRIM(source.EncounterType),
 target.PracticeName = TRIM(source.PracticeName),
 target.PvWcClaimNumberCombined = TRIM(source.PvWcClaimNumberCombined),
 target.LastTouchedDate = source.LastTouchedDate,
 target.CCUClaimSummaryDXKey = source.CCUClaimSummaryDXKey
WHEN NOT MATCHED THEN
  INSERT (SystemName, DosProviderName, ClaimNumber, EncounterID, EncounterCount, BillingArea, CoidSpecialty, COID, ClaimCoid, RegionKey, EncounterDate, EMRDXsVsClaimDXs, DxCodeChange, EncounterDXCode, ClaimDXCode, LastChangedBy34Dept, BillingNotes, PatientAccountNumber, FinancialNumber, PatientName, EncounterDateYYYYMM, LastTouchedDate_YYYYMM, EncounterCoidLob, EncounterCoidSubLob, DWLastUpdateDateTime, EncounterType, PracticeName, PvWcClaimNumberCombined, LastTouchedDate, CCUClaimSummaryDXKey)
  VALUES (TRIM(source.SystemName), TRIM(source.DosProviderName), source.ClaimNumber, source.EncounterID, source.EncounterCount, TRIM(source.BillingArea), TRIM(source.CoidSpecialty), TRIM(source.COID), TRIM(source.ClaimCoid), source.RegionKey, source.EncounterDate, TRIM(source.EMRDXsVsClaimDXs), TRIM(source.DxCodeChange), TRIM(source.EncounterDXCode), TRIM(source.ClaimDXCode), TRIM(source.LastChangedBy34Dept), TRIM(source.BillingNotes), source.PatientAccountNumber, TRIM(source.FinancialNumber), TRIM(source.PatientName), TRIM(source.EncounterDateYYYYMM), TRIM(source.LastTouchedDate_YYYYMM), TRIM(source.EncounterCoidLob), TRIM(source.EncounterCoidSubLob), source.DWLastUpdateDateTime, TRIM(source.EncounterType), TRIM(source.PracticeName), TRIM(source.PvWcClaimNumberCombined), source.LastTouchedDate, source.CCUClaimSummaryDXKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUClaimSummaryDXKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_RprtCCUClaimSummaryDX
      GROUP BY CCUClaimSummaryDXKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RprtCCUClaimSummaryDX');
ELSE
  COMMIT TRANSACTION;
END IF;
