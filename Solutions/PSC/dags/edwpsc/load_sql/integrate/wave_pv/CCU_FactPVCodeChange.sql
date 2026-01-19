
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactPVCodeChange AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactPVCodeChange AS source
ON target.CodeChangeKey = source.CodeChangeKey
WHEN MATCHED THEN
  UPDATE SET
  target.CodeChangeKey = source.CodeChangeKey,
 target.EncounterKey = source.EncounterKey,
 target.EncounterID = source.EncounterID,
 target.RegionKey = source.RegionKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.ClaimNumber = source.ClaimNumber,
 target.ClaimLineNumber = source.ClaimLineNumber,
 target.EncounterCOID = TRIM(source.EncounterCOID),
 target.COID = TRIM(source.COID),
 target.EncounterTOSCode = source.EncounterTOSCode,
 target.ClaimTOSCode = source.ClaimTOSCode,
 target.TOSCodeMatch = TRIM(source.TOSCodeMatch),
 target.TOSCodeMatchType = TRIM(source.TOSCodeMatchType),
 target.EncounterPOSCode = source.EncounterPOSCode,
 target.ClaimPOSCode = source.ClaimPOSCode,
 target.POSCodeMatch = TRIM(source.POSCodeMatch),
 target.POSCodeMatchType = TRIM(source.POSCodeMatchType),
 target.EncounterProcedureCode = TRIM(source.EncounterProcedureCode),
 target.ClaimProcedureCode = TRIM(source.ClaimProcedureCode),
 target.ProcedureCodeMatch = TRIM(source.ProcedureCodeMatch),
 target.ProcedureCodeMatchType = TRIM(source.ProcedureCodeMatchType),
 target.ClaimModifier1Code = TRIM(source.ClaimModifier1Code),
 target.EncounterUnitQty = source.EncounterUnitQty,
 target.ClaimUnitQty = source.ClaimUnitQty,
 target.UnitQtyMatch = TRIM(source.UnitQtyMatch),
 target.UnitQtyMatchType = TRIM(source.UnitQtyMatchType),
 target.EncounterProcedureOrderNum = source.EncounterProcedureOrderNum,
 target.ClaimLineItemNum = source.ClaimLineItemNum,
 target.EncounterDxPrimaryCode = TRIM(source.EncounterDxPrimaryCode),
 target.DxPrimaryCodeMatch = TRIM(source.DxPrimaryCodeMatch),
 target.DxPrimaryCodeMatchType = TRIM(source.DxPrimaryCodeMatchType),
 target.EncounterDx1Code = TRIM(source.EncounterDx1Code),
 target.EncounterDx2Code = TRIM(source.EncounterDx2Code),
 target.EncounterDx3Code = TRIM(source.EncounterDx3Code),
 target.EncounterDx4Code = TRIM(source.EncounterDx4Code),
 target.EncounterDx5Code = TRIM(source.EncounterDx5Code),
 target.EncounterDx6Code = TRIM(source.EncounterDx6Code),
 target.EncounterDx7Code = TRIM(source.EncounterDx7Code),
 target.EncounterDx8Code = TRIM(source.EncounterDx8Code),
 target.EncounterDx9Code = TRIM(source.EncounterDx9Code),
 target.EncounterDx10Code = TRIM(source.EncounterDx10Code),
 target.ClaimDx1Code = TRIM(source.ClaimDx1Code),
 target.Dx1CodeMatch = TRIM(source.Dx1CodeMatch),
 target.Dx1CodeMatchType = TRIM(source.Dx1CodeMatchType),
 target.ClaimDx2Code = TRIM(source.ClaimDx2Code),
 target.Dx2CodeMatch = TRIM(source.Dx2CodeMatch),
 target.Dx2CodeMatchType = TRIM(source.Dx2CodeMatchType),
 target.ClaimDx3Code = TRIM(source.ClaimDx3Code),
 target.Dx3CodeMatch = TRIM(source.Dx3CodeMatch),
 target.Dx3CodeMatchType = TRIM(source.Dx3CodeMatchType),
 target.EncounterDate = source.EncounterDate,
 target.ClaimFromServiceDate = source.ClaimFromServiceDate,
 target.EncounterDateToFromServiceDateMatch = TRIM(source.EncounterDateToFromServiceDateMatch),
 target.EncounterDateToFromServiceDateMatchType = TRIM(source.EncounterDateToFromServiceDateMatchType),
 target.EncounterSmryOfCareDate = source.EncounterSmryOfCareDate,
 target.ClaimStartEntryDate = source.ClaimStartEntryDate,
 target.ClaimEndEntryDate = source.ClaimEndEntryDate,
 target.Claimdate = source.Claimdate,
 target.EncounterBillingTypeCode = TRIM(source.EncounterBillingTypeCode),
 target.EncounterClaimMatch = TRIM(source.EncounterClaimMatch),
 target.ClaimCreatedByLastName = TRIM(source.ClaimCreatedByLastName),
 target.ClaimCreatedByFirstName = TRIM(source.ClaimCreatedByFirstName),
 target.ClaimCreatedBy34Id = TRIM(source.ClaimCreatedBy34Id),
 target.DosProviderId = source.DosProviderId,
 target.RenProviderId = source.RenProviderId,
 target.CreatedByLname = TRIM(source.CreatedByLname),
 target.CreatedByFname = TRIM(source.CreatedByFname),
 target.CreatedByUserKey = TRIM(source.CreatedByUserKey),
 target.SupervisorLname = TRIM(source.SupervisorLname),
 target.SupervisorFname = TRIM(source.SupervisorFname),
 target.SupervisorProviderKey = TRIM(source.SupervisorProviderKey),
 target.PracticeIdName = TRIM(source.PracticeIdName),
 target.RenProviderLname = TRIM(source.RenProviderLname),
 target.RenProviderFname = TRIM(source.RenProviderFname),
 target.RenProviderKey = TRIM(source.RenProviderKey),
 target.DosProviderLname = TRIM(source.DosProviderLname),
 target.DosProviderFname = TRIM(source.DosProviderFname),
 target.DosProviderKey = TRIM(source.DosProviderKey),
 target.ClaimStatusName = TRIM(source.ClaimStatusName),
 target.ClaimStatusLastModifiedDate = source.ClaimStatusLastModifiedDate,
 target.ClaimStatusLastModifiedUserName = TRIM(source.ClaimStatusLastModifiedUserName),
 target.EncounterType = TRIM(source.EncounterType),
 target.PracticeName = TRIM(source.PracticeName),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.HashNoMatch = source.HashNoMatch,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.LastChangedBy = source.LastChangedBy,
 target.ChangedDate = source.ChangedDate,
 target.isActive = source.isActive
WHEN NOT MATCHED THEN
  INSERT (CodeChangeKey, EncounterKey, EncounterID, RegionKey, ClaimKey, ClaimLineChargeKey, ClaimNumber, ClaimLineNumber, EncounterCOID, COID, EncounterTOSCode, ClaimTOSCode, TOSCodeMatch, TOSCodeMatchType, EncounterPOSCode, ClaimPOSCode, POSCodeMatch, POSCodeMatchType, EncounterProcedureCode, ClaimProcedureCode, ProcedureCodeMatch, ProcedureCodeMatchType, ClaimModifier1Code, EncounterUnitQty, ClaimUnitQty, UnitQtyMatch, UnitQtyMatchType, EncounterProcedureOrderNum, ClaimLineItemNum, EncounterDxPrimaryCode, DxPrimaryCodeMatch, DxPrimaryCodeMatchType, EncounterDx1Code, EncounterDx2Code, EncounterDx3Code, EncounterDx4Code, EncounterDx5Code, EncounterDx6Code, EncounterDx7Code, EncounterDx8Code, EncounterDx9Code, EncounterDx10Code, ClaimDx1Code, Dx1CodeMatch, Dx1CodeMatchType, ClaimDx2Code, Dx2CodeMatch, Dx2CodeMatchType, ClaimDx3Code, Dx3CodeMatch, Dx3CodeMatchType, EncounterDate, ClaimFromServiceDate, EncounterDateToFromServiceDateMatch, EncounterDateToFromServiceDateMatchType, EncounterSmryOfCareDate, ClaimStartEntryDate, ClaimEndEntryDate, Claimdate, EncounterBillingTypeCode, EncounterClaimMatch, ClaimCreatedByLastName, ClaimCreatedByFirstName, ClaimCreatedBy34Id, DosProviderId, RenProviderId, CreatedByLname, CreatedByFname, CreatedByUserKey, SupervisorLname, SupervisorFname, SupervisorProviderKey, PracticeIdName, RenProviderLname, RenProviderFname, RenProviderKey, DosProviderLname, DosProviderFname, DosProviderKey, ClaimStatusName, ClaimStatusLastModifiedDate, ClaimStatusLastModifiedUserName, EncounterType, PracticeName, DWLastUpdateDateTime, SourceSystemCode, HashNoMatch, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, LastChangedBy, ChangedDate, isActive)
  VALUES (source.CodeChangeKey, source.EncounterKey, source.EncounterID, source.RegionKey, source.ClaimKey, source.ClaimLineChargeKey, source.ClaimNumber, source.ClaimLineNumber, TRIM(source.EncounterCOID), TRIM(source.COID), source.EncounterTOSCode, source.ClaimTOSCode, TRIM(source.TOSCodeMatch), TRIM(source.TOSCodeMatchType), source.EncounterPOSCode, source.ClaimPOSCode, TRIM(source.POSCodeMatch), TRIM(source.POSCodeMatchType), TRIM(source.EncounterProcedureCode), TRIM(source.ClaimProcedureCode), TRIM(source.ProcedureCodeMatch), TRIM(source.ProcedureCodeMatchType), TRIM(source.ClaimModifier1Code), source.EncounterUnitQty, source.ClaimUnitQty, TRIM(source.UnitQtyMatch), TRIM(source.UnitQtyMatchType), source.EncounterProcedureOrderNum, source.ClaimLineItemNum, TRIM(source.EncounterDxPrimaryCode), TRIM(source.DxPrimaryCodeMatch), TRIM(source.DxPrimaryCodeMatchType), TRIM(source.EncounterDx1Code), TRIM(source.EncounterDx2Code), TRIM(source.EncounterDx3Code), TRIM(source.EncounterDx4Code), TRIM(source.EncounterDx5Code), TRIM(source.EncounterDx6Code), TRIM(source.EncounterDx7Code), TRIM(source.EncounterDx8Code), TRIM(source.EncounterDx9Code), TRIM(source.EncounterDx10Code), TRIM(source.ClaimDx1Code), TRIM(source.Dx1CodeMatch), TRIM(source.Dx1CodeMatchType), TRIM(source.ClaimDx2Code), TRIM(source.Dx2CodeMatch), TRIM(source.Dx2CodeMatchType), TRIM(source.ClaimDx3Code), TRIM(source.Dx3CodeMatch), TRIM(source.Dx3CodeMatchType), source.EncounterDate, source.ClaimFromServiceDate, TRIM(source.EncounterDateToFromServiceDateMatch), TRIM(source.EncounterDateToFromServiceDateMatchType), source.EncounterSmryOfCareDate, source.ClaimStartEntryDate, source.ClaimEndEntryDate, source.Claimdate, TRIM(source.EncounterBillingTypeCode), TRIM(source.EncounterClaimMatch), TRIM(source.ClaimCreatedByLastName), TRIM(source.ClaimCreatedByFirstName), TRIM(source.ClaimCreatedBy34Id), source.DosProviderId, source.RenProviderId, TRIM(source.CreatedByLname), TRIM(source.CreatedByFname), TRIM(source.CreatedByUserKey), TRIM(source.SupervisorLname), TRIM(source.SupervisorFname), TRIM(source.SupervisorProviderKey), TRIM(source.PracticeIdName), TRIM(source.RenProviderLname), TRIM(source.RenProviderFname), TRIM(source.RenProviderKey), TRIM(source.DosProviderLname), TRIM(source.DosProviderFname), TRIM(source.DosProviderKey), TRIM(source.ClaimStatusName), source.ClaimStatusLastModifiedDate, TRIM(source.ClaimStatusLastModifiedUserName), TRIM(source.EncounterType), TRIM(source.PracticeName), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), source.HashNoMatch, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.LastChangedBy, source.ChangedDate, source.isActive);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CodeChangeKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactPVCodeChange
      GROUP BY CodeChangeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactPVCodeChange');
ELSE
  COMMIT TRANSACTION;
END IF;
