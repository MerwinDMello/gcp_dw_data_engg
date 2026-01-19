
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryMeditech AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtClaimSummaryMeditech AS source
ON target.CCUClaimSummaryID = source.CCUClaimSummaryID
WHEN MATCHED THEN
  UPDATE SET
  target.CCUClaimSummaryID = source.CCUClaimSummaryID,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DosProviderKey = source.DosProviderKey,
 target.DosProviderUname = TRIM(source.DosProviderUname),
 target.DosProviderLastName = TRIM(source.DosProviderLastName),
 target.DosProviderFirstName = TRIM(source.DosProviderFirstName),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.ClaimNumber_Combined = TRIM(source.ClaimNumber_Combined),
 target.EncounterID_Combined = TRIM(source.EncounterID_Combined),
 target.EncounterCOID = TRIM(source.EncounterCOID),
 target.ClaimCOID = TRIM(source.ClaimCOID),
 target.EncounterKey_Combined = TRIM(source.EncounterKey_Combined),
 target.ClaimKey_Combined = TRIM(source.ClaimKey_Combined),
 target.RegionKey = source.RegionKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.RecentChangedDate = source.RecentChangedDate,
 target.EncounterProcedureCodeCombined = TRIM(source.EncounterProcedureCodeCombined),
 target.ClaimProcedureCodeCombined = TRIM(source.ClaimProcedureCodeCombined),
 target.EMRCptsVsClaimCpts = TRIM(source.EMRCptsVsClaimCpts),
 target.UnitQtyChanged = TRIM(source.UnitQtyChanged),
 target.EmrCodeEstValue = source.EmrCodeEstValue,
 target.ClaimCodeEstValue = source.ClaimCodeEstValue,
 target.CodeChangeImpact = source.CodeChangeImpact,
 target.LastChangedBy34Combined = TRIM(source.LastChangedBy34Combined),
 target.LastChangedByNameCombined = TRIM(source.LastChangedByNameCombined),
 target.LastChangedByDeptCombined = TRIM(source.LastChangedByDeptCombined),
 target.LastChangedByCompanyNameCombined = TRIM(source.LastChangedByCompanyNameCombined),
 target.Comments = TRIM(source.Comments),
 target.PatientMRNCombined = TRIM(source.PatientMRNCombined),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.CodeChangeImpactDirection = TRIM(source.CodeChangeImpactDirection),
 target.InscopeCategories = TRIM(source.InscopeCategories),
 target.LastChangedByDeptFlag = TRIM(source.LastChangedByDeptFlag),
 target.CCUFlag = TRIM(source.CCUFlag),
 target.EncounterDateYYYYMM = TRIM(source.EncounterDateYYYYMM),
 target.ClaimFromServiceDateYYYYMM = TRIM(source.ClaimFromServiceDateYYYYMM),
 target.LastTouchedDateYYYYMM = TRIM(source.LastTouchedDateYYYYMM),
 target.EncounterCoidLob = TRIM(source.EncounterCoidLob),
 target.EncounterCoidSubLob = TRIM(source.EncounterCoidSubLob),
 target.H2UFlag = TRIM(source.H2UFlag),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (CCUClaimSummaryID, SourceSystemCode, DosProviderKey, DosProviderUname, DosProviderLastName, DosProviderFirstName, VisitNumber, ClaimNumber_Combined, EncounterID_Combined, EncounterCOID, ClaimCOID, EncounterKey_Combined, ClaimKey_Combined, RegionKey, ServiceDateKey, RecentChangedDate, EncounterProcedureCodeCombined, ClaimProcedureCodeCombined, EMRCptsVsClaimCpts, UnitQtyChanged, EmrCodeEstValue, ClaimCodeEstValue, CodeChangeImpact, LastChangedBy34Combined, LastChangedByNameCombined, LastChangedByDeptCombined, LastChangedByCompanyNameCombined, Comments, PatientMRNCombined, PatientLastName, PatientFirstName, CodeChangeImpactDirection, InscopeCategories, LastChangedByDeptFlag, CCUFlag, EncounterDateYYYYMM, ClaimFromServiceDateYYYYMM, LastTouchedDateYYYYMM, EncounterCoidLob, EncounterCoidSubLob, H2UFlag, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.CCUClaimSummaryID, TRIM(source.SourceSystemCode), source.DosProviderKey, TRIM(source.DosProviderUname), TRIM(source.DosProviderLastName), TRIM(source.DosProviderFirstName), TRIM(source.VisitNumber), TRIM(source.ClaimNumber_Combined), TRIM(source.EncounterID_Combined), TRIM(source.EncounterCOID), TRIM(source.ClaimCOID), TRIM(source.EncounterKey_Combined), TRIM(source.ClaimKey_Combined), source.RegionKey, source.ServiceDateKey, source.RecentChangedDate, TRIM(source.EncounterProcedureCodeCombined), TRIM(source.ClaimProcedureCodeCombined), TRIM(source.EMRCptsVsClaimCpts), TRIM(source.UnitQtyChanged), source.EmrCodeEstValue, source.ClaimCodeEstValue, source.CodeChangeImpact, TRIM(source.LastChangedBy34Combined), TRIM(source.LastChangedByNameCombined), TRIM(source.LastChangedByDeptCombined), TRIM(source.LastChangedByCompanyNameCombined), TRIM(source.Comments), TRIM(source.PatientMRNCombined), TRIM(source.PatientLastName), TRIM(source.PatientFirstName), TRIM(source.CodeChangeImpactDirection), TRIM(source.InscopeCategories), TRIM(source.LastChangedByDeptFlag), TRIM(source.CCUFlag), TRIM(source.EncounterDateYYYYMM), TRIM(source.ClaimFromServiceDateYYYYMM), TRIM(source.LastTouchedDateYYYYMM), TRIM(source.EncounterCoidLob), TRIM(source.EncounterCoidSubLob), TRIM(source.H2UFlag), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUClaimSummaryID
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryMeditech
      GROUP BY CCUClaimSummaryID
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtClaimSummaryMeditech');
ELSE
  COMMIT TRANSACTION;
END IF;
