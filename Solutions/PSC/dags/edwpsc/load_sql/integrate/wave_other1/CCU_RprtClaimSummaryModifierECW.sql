
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryModifierECW AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtClaimSummaryModifierECW AS source
ON target.EncounterID = source.EncounterID AND target.RegionKey = source.RegionKey AND target.SystemName = source.SystemName
WHEN MATCHED THEN
  UPDATE SET
  target.SystemName = TRIM(source.SystemName),
 target.DosProviderName = TRIM(source.DosProviderName),
 target.RegionName = TRIM(source.RegionName),
 target.ClaimNumber = source.ClaimNumber,
 target.EncounterID = source.EncounterID,
 target.EncounterCount = source.EncounterCount,
 target.BillingArea = TRIM(source.BillingArea),
 target.CoidSpecialty = TRIM(source.CoidSpecialty),
 target.COID = TRIM(source.COID),
 target.ClaimCoid = TRIM(source.ClaimCoid),
 target.RegionKey = source.RegionKey,
 target.EncounterDate = source.EncounterDate,
 target.EmrModifierVsClaimModifier = TRIM(source.EmrModifierVsClaimModifier),
 target.ModifierCodeChange = TRIM(source.ModifierCodeChange),
 target.EncounterModifier = TRIM(source.EncounterModifier),
 target.ClaimModifier = TRIM(source.ClaimModifier),
 target.LastChangedBy34 = TRIM(source.LastChangedBy34),
 target.LastChangedByDept = TRIM(source.LastChangedByDept),
 target.BillingNotes = TRIM(source.BillingNotes),
 target.PatientAccountNumber = TRIM(source.PatientAccountNumber),
 target.FinancialNumber = TRIM(source.FinancialNumber),
 target.PatientName = TRIM(source.PatientName),
 target.EncounterDateYYYYMM = TRIM(source.EncounterDateYYYYMM),
 target.LastTouchedDate_YYYYMM = TRIM(source.LastTouchedDate_YYYYMM),
 target.EncounterCoidLob = TRIM(source.EncounterCoidLob),
 target.EncounterCoidSubLob = TRIM(source.EncounterCoidSubLob),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SystemName, DosProviderName, RegionName, ClaimNumber, EncounterID, EncounterCount, BillingArea, CoidSpecialty, COID, ClaimCoid, RegionKey, EncounterDate, EmrModifierVsClaimModifier, ModifierCodeChange, EncounterModifier, ClaimModifier, LastChangedBy34, LastChangedByDept, BillingNotes, PatientAccountNumber, FinancialNumber, PatientName, EncounterDateYYYYMM, LastTouchedDate_YYYYMM, EncounterCoidLob, EncounterCoidSubLob, DWLastUpdateDateTime)
  VALUES (TRIM(source.SystemName), TRIM(source.DosProviderName), TRIM(source.RegionName), source.ClaimNumber, source.EncounterID, source.EncounterCount, TRIM(source.BillingArea), TRIM(source.CoidSpecialty), TRIM(source.COID), TRIM(source.ClaimCoid), source.RegionKey, source.EncounterDate, TRIM(source.EmrModifierVsClaimModifier), TRIM(source.ModifierCodeChange), TRIM(source.EncounterModifier), TRIM(source.ClaimModifier), TRIM(source.LastChangedBy34), TRIM(source.LastChangedByDept), TRIM(source.BillingNotes), TRIM(source.PatientAccountNumber), TRIM(source.FinancialNumber), TRIM(source.PatientName), TRIM(source.EncounterDateYYYYMM), TRIM(source.LastTouchedDate_YYYYMM), TRIM(source.EncounterCoidLob), TRIM(source.EncounterCoidSubLob), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterID, RegionKey, SystemName
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryModifierECW
      GROUP BY EncounterID, RegionKey, SystemName
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryModifierECW');
ELSE
  COMMIT TRANSACTION;
END IF;
