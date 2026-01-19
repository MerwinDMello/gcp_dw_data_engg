
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactEPICCodeChange AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactEPICCodeChange AS source
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
 target.ClaimCOID = TRIM(source.ClaimCOID),
 target.EncounterTOSCode = TRIM(source.EncounterTOSCode),
 target.ClaimTOSCode = source.ClaimTOSCode,
 target.TOSCodeMatch = TRIM(source.TOSCodeMatch),
 target.TOSCodeMatchType = TRIM(source.TOSCodeMatchType),
 target.EncounterPOSCode = source.EncounterPOSCode,
 target.ClaimPOSCode = TRIM(source.ClaimPOSCode),
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
 target.ClaimLineItemNum = source.ClaimLineItemNum,
 target.ClaimDx1Code = TRIM(source.ClaimDx1Code),
 target.ClaimDx2Code = TRIM(source.ClaimDx2Code),
 target.ClaimDx3Code = TRIM(source.ClaimDx3Code),
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
 target.RenProviderId = TRIM(source.RenProviderId),
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
 target.TransactionNumber = source.TransactionNumber,
 target.VisitNumber = source.VisitNumber,
 target.PatientKey = source.PatientKey,
 target.PayToProviderKey = source.PayToProviderKey,
 target.ServiceAreaKey = source.ServiceAreaKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.HashNoMatch = source.HashNoMatch,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.LastChangedBy = source.LastChangedBy,
 target.ChangedDate = source.ChangedDate,
 target.isActive = source.isActive,
 target.EncounterProcedureOrderNum = source.EncounterProcedureOrderNum
WHEN NOT MATCHED THEN
  INSERT (CodeChangeKey, EncounterKey, EncounterID, RegionKey, ClaimKey, ClaimLineChargeKey, ClaimNumber, ClaimLineNumber, EncounterCOID, ClaimCOID, EncounterTOSCode, ClaimTOSCode, TOSCodeMatch, TOSCodeMatchType, EncounterPOSCode, ClaimPOSCode, POSCodeMatch, POSCodeMatchType, EncounterProcedureCode, ClaimProcedureCode, ProcedureCodeMatch, ProcedureCodeMatchType, ClaimModifier1Code, EncounterUnitQty, ClaimUnitQty, UnitQtyMatch, UnitQtyMatchType, ClaimLineItemNum, ClaimDx1Code, ClaimDx2Code, ClaimDx3Code, EncounterDate, ClaimFromServiceDate, EncounterDateToFromServiceDateMatch, EncounterDateToFromServiceDateMatchType, EncounterSmryOfCareDate, ClaimStartEntryDate, ClaimEndEntryDate, Claimdate, EncounterBillingTypeCode, EncounterClaimMatch, ClaimCreatedByLastName, ClaimCreatedByFirstName, ClaimCreatedBy34Id, DosProviderId, RenProviderId, CreatedByLname, CreatedByFname, CreatedByUserKey, SupervisorLname, SupervisorFname, SupervisorProviderKey, PracticeIdName, RenProviderLname, RenProviderFname, RenProviderKey, DosProviderLname, DosProviderFname, DosProviderKey, ClaimStatusName, ClaimStatusLastModifiedDate, ClaimStatusLastModifiedUserName, EncounterType, PracticeName, TransactionNumber, VisitNumber, PatientKey, PayToProviderKey, ServiceAreaKey, DWLastUpdateDateTime, SourceSystemCode, HashNoMatch, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, LastChangedBy, ChangedDate, isActive, EncounterProcedureOrderNum)
  VALUES (source.CodeChangeKey, source.EncounterKey, source.EncounterID, source.RegionKey, source.ClaimKey, source.ClaimLineChargeKey, source.ClaimNumber, source.ClaimLineNumber, TRIM(source.EncounterCOID), TRIM(source.ClaimCOID), TRIM(source.EncounterTOSCode), source.ClaimTOSCode, TRIM(source.TOSCodeMatch), TRIM(source.TOSCodeMatchType), source.EncounterPOSCode, TRIM(source.ClaimPOSCode), TRIM(source.POSCodeMatch), TRIM(source.POSCodeMatchType), TRIM(source.EncounterProcedureCode), TRIM(source.ClaimProcedureCode), TRIM(source.ProcedureCodeMatch), TRIM(source.ProcedureCodeMatchType), TRIM(source.ClaimModifier1Code), source.EncounterUnitQty, source.ClaimUnitQty, TRIM(source.UnitQtyMatch), TRIM(source.UnitQtyMatchType), source.ClaimLineItemNum, TRIM(source.ClaimDx1Code), TRIM(source.ClaimDx2Code), TRIM(source.ClaimDx3Code), source.EncounterDate, source.ClaimFromServiceDate, TRIM(source.EncounterDateToFromServiceDateMatch), TRIM(source.EncounterDateToFromServiceDateMatchType), source.EncounterSmryOfCareDate, source.ClaimStartEntryDate, source.ClaimEndEntryDate, source.Claimdate, TRIM(source.EncounterBillingTypeCode), TRIM(source.EncounterClaimMatch), TRIM(source.ClaimCreatedByLastName), TRIM(source.ClaimCreatedByFirstName), TRIM(source.ClaimCreatedBy34Id), source.DosProviderId, TRIM(source.RenProviderId), TRIM(source.CreatedByLname), TRIM(source.CreatedByFname), TRIM(source.CreatedByUserKey), TRIM(source.SupervisorLname), TRIM(source.SupervisorFname), TRIM(source.SupervisorProviderKey), TRIM(source.PracticeIdName), TRIM(source.RenProviderLname), TRIM(source.RenProviderFname), TRIM(source.RenProviderKey), TRIM(source.DosProviderLname), TRIM(source.DosProviderFname), TRIM(source.DosProviderKey), TRIM(source.ClaimStatusName), source.ClaimStatusLastModifiedDate, TRIM(source.ClaimStatusLastModifiedUserName), TRIM(source.EncounterType), TRIM(source.PracticeName), source.TransactionNumber, source.VisitNumber, source.PatientKey, source.PayToProviderKey, source.ServiceAreaKey, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), source.HashNoMatch, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.LastChangedBy, source.ChangedDate, source.isActive, source.EncounterProcedureOrderNum);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CodeChangeKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactEPICCodeChange
      GROUP BY CodeChangeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactEPICCodeChange');
ELSE
  COMMIT TRANSACTION;
END IF;
