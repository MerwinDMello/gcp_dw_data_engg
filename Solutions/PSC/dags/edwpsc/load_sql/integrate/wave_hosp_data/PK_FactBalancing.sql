
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactBalancing AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactBalancing AS source
ON target.PKBId = source.PKBId
WHEN MATCHED THEN
  UPDATE SET
  target.PKBId = source.PKBId,
 target.RegionKey = source.RegionKey,
 target.COID = TRIM(source.COID),
 target.CPTFoundInECW = TRIM(source.CPTFoundInECW),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.BatchNumber = TRIM(source.BatchNumber),
 target.FirstMessageID = source.FirstMessageID,
 target.LastMessageID = source.LastMessageID,
 target.PMID = source.PMID,
 target.FirstDateReceived = source.FirstDateReceived,
 target.LastDateReceived = source.LastDateReceived,
 target.FirstClaimnumber = source.FirstClaimnumber,
 target.LastClaimnumber = source.LastClaimnumber,
 target.FirstMessageStatus = source.FirstMessageStatus,
 target.LastMessageStatus = source.LastMessageStatus,
 target.LastErrorMessage = TRIM(source.LastErrorMessage),
 target.Doctorid = source.Doctorid,
 target.ReconciledFlag = source.ReconciledFlag,
 target.Facilityid = source.Facilityid,
 target.PracticeidFromResults = TRIM(source.PracticeidFromResults),
 target.ProcedureDateFromHl7 = TRIM(source.ProcedureDateFromHl7),
 target.DOBFromHl7 = TRIM(source.DOBFromHl7),
 target.PatientFirstNameFromHl7 = TRIM(source.PatientFirstNameFromHl7),
 target.PatientLastNameFromHl7 = TRIM(source.PatientLastNameFromHl7),
 target.PatientIDFoundFromHl7 = source.PatientIDFoundFromHl7,
 target.ProviderNPIFromHl7 = TRIM(source.ProviderNPIFromHl7),
 target.ProviderIDFoundFromHl7 = source.ProviderIDFoundFromHl7,
 target.ProcedureCodeFromHl7 = TRIM(source.ProcedureCodeFromHl7),
 target.ClaimNumberFoundFromHl7Lookup = source.ClaimNumberFoundFromHl7Lookup,
 target.CPTIdFoundFromHl7Lookup = source.CPTIdFoundFromHl7Lookup,
 target.CreatedByFromHl7Lookup = TRIM(source.CreatedByFromHl7Lookup),
 target.ClaimPatientIDFound2FromResultsLookup = source.ClaimPatientIDFound2FromResultsLookup,
 target.PatientFirstNameResultsLookup = TRIM(source.PatientFirstNameResultsLookup),
 target.PatientLastNameResultsLookup = TRIM(source.PatientLastNameResultsLookup),
 target.PatientDOBResultsLookup = TRIM(source.PatientDOBResultsLookup),
 target.ClaimServiceDtResultsLookup = source.ClaimServiceDtResultsLookup,
 target.ClaimNumberFound2FromResults = source.ClaimNumberFound2FromResults,
 target.CPTIdFound2FromResultsLookup = source.CPTIdFound2FromResultsLookup,
 target.CreatedBy2FromResultsLookup = TRIM(source.CreatedBy2FromResultsLookup),
 target.CreatedByID2FromResultsLookup = source.CreatedByID2FromResultsLookup,
 target.CreatedByIDFromHl7Lookup = source.CreatedByIDFromHl7Lookup,
 target.ClaimNumber = source.ClaimNumber,
 target.VoidFlagClaim = source.VoidFlagClaim,
 target.DeleteFlagClaim = source.DeleteFlagClaim,
 target.PatientID = source.PatientID,
 target.CreatedByID = source.CreatedByID,
 target.InBalanceFlag = source.InBalanceFlag,
 target.ClaimCreatedBySystemFlag = source.ClaimCreatedBySystemFlag,
 target.ClaimCreatedByUserFlag = source.ClaimCreatedByUserFlag,
 target.HashCPT = source.HashCPT,
 target.FacilityCodeFromHL7 = TRIM(source.FacilityCodeFromHL7),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ProviderNameFromHL7 = TRIM(source.ProviderNameFromHL7),
 target.IsIgnoredFlag = source.IsIgnoredFlag,
 target.ActionByUserName = TRIM(source.ActionByUserName),
 target.ActionDateTime = source.ActionDateTime,
 target.ClaimNumberFound3 = source.ClaimNumberFound3,
 target.ReconModifiedDate = source.ReconModifiedDate,
 target.ReconUserID = TRIM(source.ReconUserID),
 target.ReconClaimID = source.ReconClaimID,
 target.ReconNotes = TRIM(source.ReconNotes),
 target.MessageControlId = TRIM(source.MessageControlId),
 target.ChargeTransactionId = TRIM(source.ChargeTransactionId),
 target.PKRegionName = TRIM(source.PKRegionName)
WHEN NOT MATCHED THEN
  INSERT (PKBId, RegionKey, COID, CPTFoundInECW, VisitNumber, BatchNumber, FirstMessageID, LastMessageID, PMID, FirstDateReceived, LastDateReceived, FirstClaimnumber, LastClaimnumber, FirstMessageStatus, LastMessageStatus, LastErrorMessage, Doctorid, ReconciledFlag, Facilityid, PracticeidFromResults, ProcedureDateFromHl7, DOBFromHl7, PatientFirstNameFromHl7, PatientLastNameFromHl7, PatientIDFoundFromHl7, ProviderNPIFromHl7, ProviderIDFoundFromHl7, ProcedureCodeFromHl7, ClaimNumberFoundFromHl7Lookup, CPTIdFoundFromHl7Lookup, CreatedByFromHl7Lookup, ClaimPatientIDFound2FromResultsLookup, PatientFirstNameResultsLookup, PatientLastNameResultsLookup, PatientDOBResultsLookup, ClaimServiceDtResultsLookup, ClaimNumberFound2FromResults, CPTIdFound2FromResultsLookup, CreatedBy2FromResultsLookup, CreatedByID2FromResultsLookup, CreatedByIDFromHl7Lookup, ClaimNumber, VoidFlagClaim, DeleteFlagClaim, PatientID, CreatedByID, InBalanceFlag, ClaimCreatedBySystemFlag, ClaimCreatedByUserFlag, HashCPT, FacilityCodeFromHL7, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ProviderNameFromHL7, IsIgnoredFlag, ActionByUserName, ActionDateTime, ClaimNumberFound3, ReconModifiedDate, ReconUserID, ReconClaimID, ReconNotes, MessageControlId, ChargeTransactionId, PKRegionName)
  VALUES (source.PKBId, source.RegionKey, TRIM(source.COID), TRIM(source.CPTFoundInECW), TRIM(source.VisitNumber), TRIM(source.BatchNumber), source.FirstMessageID, source.LastMessageID, source.PMID, source.FirstDateReceived, source.LastDateReceived, source.FirstClaimnumber, source.LastClaimnumber, source.FirstMessageStatus, source.LastMessageStatus, TRIM(source.LastErrorMessage), source.Doctorid, source.ReconciledFlag, source.Facilityid, TRIM(source.PracticeidFromResults), TRIM(source.ProcedureDateFromHl7), TRIM(source.DOBFromHl7), TRIM(source.PatientFirstNameFromHl7), TRIM(source.PatientLastNameFromHl7), source.PatientIDFoundFromHl7, TRIM(source.ProviderNPIFromHl7), source.ProviderIDFoundFromHl7, TRIM(source.ProcedureCodeFromHl7), source.ClaimNumberFoundFromHl7Lookup, source.CPTIdFoundFromHl7Lookup, TRIM(source.CreatedByFromHl7Lookup), source.ClaimPatientIDFound2FromResultsLookup, TRIM(source.PatientFirstNameResultsLookup), TRIM(source.PatientLastNameResultsLookup), TRIM(source.PatientDOBResultsLookup), source.ClaimServiceDtResultsLookup, source.ClaimNumberFound2FromResults, source.CPTIdFound2FromResultsLookup, TRIM(source.CreatedBy2FromResultsLookup), source.CreatedByID2FromResultsLookup, source.CreatedByIDFromHl7Lookup, source.ClaimNumber, source.VoidFlagClaim, source.DeleteFlagClaim, source.PatientID, source.CreatedByID, source.InBalanceFlag, source.ClaimCreatedBySystemFlag, source.ClaimCreatedByUserFlag, source.HashCPT, TRIM(source.FacilityCodeFromHL7), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ProviderNameFromHL7), source.IsIgnoredFlag, TRIM(source.ActionByUserName), source.ActionDateTime, source.ClaimNumberFound3, source.ReconModifiedDate, TRIM(source.ReconUserID), source.ReconClaimID, TRIM(source.ReconNotes), TRIM(source.MessageControlId), TRIM(source.ChargeTransactionId), TRIM(source.PKRegionName));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKBId
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactBalancing
      GROUP BY PKBId
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactBalancing');
ELSE
  COMMIT TRANSACTION;
END IF;
