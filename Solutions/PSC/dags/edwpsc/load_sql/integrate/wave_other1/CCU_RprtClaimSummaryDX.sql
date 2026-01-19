
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryDX AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtClaimSummaryDX AS source
ON target.EncounterID = source.EncounterID AND target.ClaimNumber = source.ClaimNumber AND target.RegionKey = source.RegionKey AND target.SystemName = source.SystemName
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
 target.DXCodeChange = TRIM(source.DXCodeChange),
 target.EncounterDXCode = TRIM(source.EncounterDXCode),
 target.ClaimDXCode = TRIM(source.ClaimDXCode),
 target.LastChangedBy34Dept = TRIM(source.LastChangedBy34Dept),
 target.BillingNotes = TRIM(source.BillingNotes),
 target.PatientAccountNumber = TRIM(source.PatientAccountNumber),
 target.FinancialNumber = TRIM(source.FinancialNumber),
 target.PatientName = TRIM(source.PatientName),
 target.EncounterDateYYYYMM = TRIM(source.EncounterDateYYYYMM),
 target.LastTouchedDate_YYYYMM = TRIM(source.LastTouchedDate_YYYYMM),
 target.EncounterCoidLob = TRIM(source.EncounterCoidLob),
 target.EncounterCoidSubLob = TRIM(source.EncounterCoidSubLob),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.LastTouchedDate = source.LastTouchedDate
WHEN NOT MATCHED THEN
  INSERT (SystemName, DosProviderName, ClaimNumber, EncounterID, EncounterCount, BillingArea, CoidSpecialty, COID, ClaimCoid, RegionKey, EncounterDate, EMRDXsVsClaimDXs, DXCodeChange, EncounterDXCode, ClaimDXCode, LastChangedBy34Dept, BillingNotes, PatientAccountNumber, FinancialNumber, PatientName, EncounterDateYYYYMM, LastTouchedDate_YYYYMM, EncounterCoidLob, EncounterCoidSubLob, DWLastUpdateDateTime, LastTouchedDate)
  VALUES (TRIM(source.SystemName), TRIM(source.DosProviderName), source.ClaimNumber, source.EncounterID, source.EncounterCount, TRIM(source.BillingArea), TRIM(source.CoidSpecialty), TRIM(source.COID), TRIM(source.ClaimCoid), source.RegionKey, source.EncounterDate, TRIM(source.EMRDXsVsClaimDXs), TRIM(source.DXCodeChange), TRIM(source.EncounterDXCode), TRIM(source.ClaimDXCode), TRIM(source.LastChangedBy34Dept), TRIM(source.BillingNotes), TRIM(source.PatientAccountNumber), TRIM(source.FinancialNumber), TRIM(source.PatientName), TRIM(source.EncounterDateYYYYMM), TRIM(source.LastTouchedDate_YYYYMM), TRIM(source.EncounterCoidLob), TRIM(source.EncounterCoidSubLob), source.DWLastUpdateDateTime, source.LastTouchedDate);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterID, ClaimNumber, RegionKey, SystemName
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryDX
      GROUP BY EncounterID, ClaimNumber, RegionKey, SystemName
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryDX');
ELSE
  COMMIT TRANSACTION;
END IF;
