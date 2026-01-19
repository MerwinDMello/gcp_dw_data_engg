
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummary AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtClaimSummary AS source
ON target.CCUClaimSummaryKey = source.CCUClaimSummaryKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUClaimSummaryKey = source.CCUClaimSummaryKey,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.RegionName = TRIM(source.RegionName),
 target.DosProviderKey = source.DosProviderKey,
 target.DosProviderUname = TRIM(source.DosProviderUname),
 target.DosProviderID = source.DosProviderID,
 target.DosProviderLastName = TRIM(source.DosProviderLastName),
 target.DosProviderFirstName = TRIM(source.DosProviderFirstName),
 target.ClaimNumber = source.ClaimNumber,
 target.EncounterID = source.EncounterID,
 target.BillingArea = TRIM(source.BillingArea),
 target.EncounterCOID = TRIM(source.EncounterCOID),
 target.ClaimCOID = TRIM(source.ClaimCOID),
 target.EncounterKey = source.EncounterKey,
 target.ClaimKey = source.ClaimKey,
 target.RegionKey = source.RegionKey,
 target.EncounterDateMin = source.EncounterDateMin,
 target.EncounterDateMax = source.EncounterDateMax,
 target.ClaimFromServiceDateMin = source.ClaimFromServiceDateMin,
 target.ClaimFromServiceDateMax = source.ClaimFromServiceDateMax,
 target.RecentChangedDate = source.RecentChangedDate,
 target.EncounterProcedureCodeCombined = TRIM(source.EncounterProcedureCodeCombined),
 target.ClaimProcedureCodeCombined = TRIM(source.ClaimProcedureCodeCombined),
 target.EMRCptsVsClaimCpts = TRIM(source.EMRCptsVsClaimCpts),
 target.EMRCptsNotOnClaim = TRIM(source.EMRCptsNotOnClaim),
 target.ClaimCptsNotOnEMR = TRIM(source.ClaimCptsNotOnEMR),
 target.UnitQtyChanged = TRIM(source.UnitQtyChanged),
 target.EmrCodeEstValue = source.EmrCodeEstValue,
 target.ClaimCodeEstValue = source.ClaimCodeEstValue,
 target.CodeChangeImpact = source.CodeChangeImpact,
 target.CreatedBy34id = TRIM(source.CreatedBy34id),
 target.CreatedByName = TRIM(source.CreatedByName),
 target.LastChangedBy34Combined = TRIM(source.LastChangedBy34Combined),
 target.LastChangedByNameCombined = TRIM(source.LastChangedByNameCombined),
 target.LastChangedByDeptCombined = TRIM(source.LastChangedByDeptCombined),
 target.LastChangedByCompanyNameCombined = TRIM(source.LastChangedByCompanyNameCombined),
 target.EcwBillingNotes = TRIM(source.EcwBillingNotes),
 target.PatientMRNCombined = TRIM(source.PatientMRNCombined),
 target.FinancialNumber = TRIM(source.FinancialNumber),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.CodeChangeImpactDirection = TRIM(source.CodeChangeImpactDirection),
 target.InscopeCategories = TRIM(source.InscopeCategories),
 target.LastChangedByDeptFlag = TRIM(source.LastChangedByDeptFlag),
 target.CCUFlag = TRIM(source.CCUFlag),
 target.BillingNotesConverted = TRIM(source.BillingNotesConverted),
 target.EncounterDateYYYYMM = TRIM(source.EncounterDateYYYYMM),
 target.ClaimFromServiceDateYYYYMM = TRIM(source.ClaimFromServiceDateYYYYMM),
 target.LastTouchedDateYYYYMM = TRIM(source.LastTouchedDateYYYYMM),
 target.EncounterCoidLob = TRIM(source.EncounterCoidLob),
 target.H2UFlag = TRIM(source.H2UFlag),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.EncounterCoidSubLob = TRIM(source.EncounterCoidSubLob)
WHEN NOT MATCHED THEN
  INSERT (CCUClaimSummaryKey, SourceSystemCode, RegionName, DosProviderKey, DosProviderUname, DosProviderID, DosProviderLastName, DosProviderFirstName, ClaimNumber, EncounterID, BillingArea, EncounterCOID, ClaimCOID, EncounterKey, ClaimKey, RegionKey, EncounterDateMin, EncounterDateMax, ClaimFromServiceDateMin, ClaimFromServiceDateMax, RecentChangedDate, EncounterProcedureCodeCombined, ClaimProcedureCodeCombined, EMRCptsVsClaimCpts, EMRCptsNotOnClaim, ClaimCptsNotOnEMR, UnitQtyChanged, EmrCodeEstValue, ClaimCodeEstValue, CodeChangeImpact, CreatedBy34id, CreatedByName, LastChangedBy34Combined, LastChangedByNameCombined, LastChangedByDeptCombined, LastChangedByCompanyNameCombined, EcwBillingNotes, PatientMRNCombined, FinancialNumber, PatientLastName, PatientFirstName, CodeChangeImpactDirection, InscopeCategories, LastChangedByDeptFlag, CCUFlag, BillingNotesConverted, EncounterDateYYYYMM, ClaimFromServiceDateYYYYMM, LastTouchedDateYYYYMM, EncounterCoidLob, H2UFlag, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, EncounterCoidSubLob)
  VALUES (source.CCUClaimSummaryKey, TRIM(source.SourceSystemCode), TRIM(source.RegionName), source.DosProviderKey, TRIM(source.DosProviderUname), source.DosProviderID, TRIM(source.DosProviderLastName), TRIM(source.DosProviderFirstName), source.ClaimNumber, source.EncounterID, TRIM(source.BillingArea), TRIM(source.EncounterCOID), TRIM(source.ClaimCOID), source.EncounterKey, source.ClaimKey, source.RegionKey, source.EncounterDateMin, source.EncounterDateMax, source.ClaimFromServiceDateMin, source.ClaimFromServiceDateMax, source.RecentChangedDate, TRIM(source.EncounterProcedureCodeCombined), TRIM(source.ClaimProcedureCodeCombined), TRIM(source.EMRCptsVsClaimCpts), TRIM(source.EMRCptsNotOnClaim), TRIM(source.ClaimCptsNotOnEMR), TRIM(source.UnitQtyChanged), source.EmrCodeEstValue, source.ClaimCodeEstValue, source.CodeChangeImpact, TRIM(source.CreatedBy34id), TRIM(source.CreatedByName), TRIM(source.LastChangedBy34Combined), TRIM(source.LastChangedByNameCombined), TRIM(source.LastChangedByDeptCombined), TRIM(source.LastChangedByCompanyNameCombined), TRIM(source.EcwBillingNotes), TRIM(source.PatientMRNCombined), TRIM(source.FinancialNumber), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.CodeChangeImpactDirection), TRIM(source.InscopeCategories), TRIM(source.LastChangedByDeptFlag), TRIM(source.CCUFlag), TRIM(source.BillingNotesConverted), TRIM(source.EncounterDateYYYYMM), TRIM(source.ClaimFromServiceDateYYYYMM), TRIM(source.LastTouchedDateYYYYMM), TRIM(source.EncounterCoidLob), TRIM(source.H2UFlag), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.EncounterCoidSubLob));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUClaimSummaryKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummary
      GROUP BY CCUClaimSummaryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummary');
ELSE
  COMMIT TRANSACTION;
END IF;
