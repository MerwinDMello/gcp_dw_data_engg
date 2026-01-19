
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtEPICClaimSummaryDX AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtEPICClaimSummaryDX AS source
ON target.EncounterID = source.EncounterID AND target.VisitNumber = source.VisitNumber AND target.ClaimNumber = source.ClaimNumber AND target.COID = source.COID AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.SystemName = TRIM(source.SystemName),
 target.RegionName = TRIM(source.RegionName),
 target.DosProviderName = TRIM(source.DosProviderName),
 target.ProviderSpeciality = TRIM(source.ProviderSpeciality),
 target.DosProviderUname = TRIM(source.DosProviderUname),
 target.ClaimNumber = source.ClaimNumber,
 target.EncounterID = source.EncounterID,
 target.EncounterCount = source.EncounterCount,
 target.BillingArea = TRIM(source.BillingArea),
 target.CoidSpecialty = TRIM(source.CoidSpecialty),
 target.COID = TRIM(source.COID),
 target.ClaimCOID = TRIM(source.ClaimCOID),
 target.RegionKey = source.RegionKey,
 target.EncounterDate = source.EncounterDate,
 target.LastTouchedDateYYYYMM = TRIM(source.LastTouchedDateYYYYMM),
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
 target.EncounterCoidLob = TRIM(source.EncounterCoidLob),
 target.EncounterCoidSubLob = TRIM(source.EncounterCoidSubLob),
 target.DWLastUpdateDate = source.DWLastUpdateDate,
 target.VisitNumber = source.VisitNumber,
 target.TransactionNumberCombined = TRIM(source.TransactionNumberCombined),
 target.PatientInternalIdCombined = TRIM(source.PatientInternalIdCombined),
 target.LastTouchedDate = source.LastTouchedDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SystemName, RegionName, DosProviderName, ProviderSpeciality, DosProviderUname, ClaimNumber, EncounterID, EncounterCount, BillingArea, CoidSpecialty, COID, ClaimCOID, RegionKey, EncounterDate, LastTouchedDateYYYYMM, EMRDXsVsClaimDXs, DXCodeChange, EncounterDXCode, ClaimDXCode, LastChangedBy34Dept, BillingNotes, PatientAccountNumber, FinancialNumber, PatientName, EncounterDateYYYYMM, EncounterCoidLob, EncounterCoidSubLob, DWLastUpdateDate, VisitNumber, TransactionNumberCombined, PatientInternalIdCombined, LastTouchedDate, DWLastUpdateDateTime)
  VALUES (TRIM(source.SystemName), TRIM(source.RegionName), TRIM(source.DosProviderName), TRIM(source.ProviderSpeciality), TRIM(source.DosProviderUname), source.ClaimNumber, source.EncounterID, source.EncounterCount, TRIM(source.BillingArea), TRIM(source.CoidSpecialty), TRIM(source.COID), TRIM(source.ClaimCOID), source.RegionKey, source.EncounterDate, TRIM(source.LastTouchedDateYYYYMM), TRIM(source.EMRDXsVsClaimDXs), TRIM(source.DXCodeChange), TRIM(source.EncounterDXCode), TRIM(source.ClaimDXCode), TRIM(source.LastChangedBy34Dept), TRIM(source.BillingNotes), TRIM(source.PatientAccountNumber), TRIM(source.FinancialNumber), TRIM(source.PatientName), TRIM(source.EncounterDateYYYYMM), TRIM(source.EncounterCoidLob), TRIM(source.EncounterCoidSubLob), source.DWLastUpdateDate, source.VisitNumber, TRIM(source.TransactionNumberCombined), TRIM(source.PatientInternalIdCombined), source.LastTouchedDate, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterID, VisitNumber, ClaimNumber, COID, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtEPICClaimSummaryDX
      GROUP BY EncounterID, VisitNumber, ClaimNumber, COID, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtEPICClaimSummaryDX');
ELSE
  COMMIT TRANSACTION;
END IF;
