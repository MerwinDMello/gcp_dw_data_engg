
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtOSC AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtOSC AS source
ON target.OSCKey = source.OSCKey
WHEN MATCHED THEN
  UPDATE SET
  target.OSCKey = source.OSCKey,
 target.eCWClaimNumber = TRIM(source.eCWClaimNumber),
 target.AccountNumber = TRIM(source.AccountNumber),
 target.EncounterID = TRIM(source.EncounterID),
 target.AccountPhase = TRIM(source.AccountPhase),
 target.StatusCode = TRIM(source.StatusCode),
 target.LastPoolAccountWasInAtPoolBuild = TRIM(source.LastPoolAccountWasInAtPoolBuild),
 target.PreviousPoolKey = TRIM(source.PreviousPoolKey),
 target.LastPoolStartDate = source.LastPoolStartDate,
 target.PoolType = TRIM(source.PoolType),
 target.PoolDept = TRIM(source.PoolDept),
 target.PoolDescription = TRIM(source.PoolDescription),
 target.PoolSubGroup = TRIM(source.PoolSubGroup),
 target.PoolFunction = TRIM(source.PoolFunction),
 target.PoolSLA = source.PoolSLA,
 target.PayerSequenceNumber = source.PayerSequenceNumber,
 target.PayerID = TRIM(source.PayerID),
 target.PayerName = TRIM(source.PayerName),
 target.BillDate = TRIM(source.BillDate),
 target.ClaimSubmissionDate = TRIM(source.ClaimSubmissionDate),
 target.LastPoolStartAge = source.LastPoolStartAge,
 target.SubCatAccountBalance = source.SubCatAccountBalance,
 target.ResponseAge = source.ResponseAge,
 target.SubcategoryStatusCode = TRIM(source.SubcategoryStatusCode),
 target.SubcategoryCode = TRIM(source.SubcategoryCode),
 target.SubcategoryDescription = TRIM(source.SubcategoryDescription),
 target.SubcategorySourceDept = TRIM(source.SubcategorySourceDept),
 target.SubcategoryCreateDate = TRIM(source.SubcategoryCreateDate),
 target.SubcategoryResponseDate = TRIM(source.SubcategoryResponseDate),
 target.SubcategoryQuestionDate = TRIM(source.SubcategoryQuestionDate),
 target.ServiceDate = TRIM(source.ServiceDate),
 target.eCWClaimStatus = TRIM(source.eCWClaimStatus),
 target.eCWClaimStatusDescription = TRIM(source.eCWClaimStatusDescription),
 target.SpecialInternalAgency = TRIM(source.SpecialInternalAgency),
 target.PlaceOfService = TRIM(source.PlaceOfService),
 target.PlaceOfServiceDesc = TRIM(source.PlaceOfServiceDesc),
 target.ClaimBalance = source.ClaimBalance,
 target.RenderingProviderFullName = TRIM(source.RenderingProviderFullName),
 target.RenderingProviderNPI = TRIM(source.RenderingProviderNPI),
 target.eCWRegion = source.eCWRegion,
 target.eCWGroup = TRIM(source.eCWGroup),
 target.eCWDivision = TRIM(source.eCWDivision),
 target.eCWMarket = TRIM(source.eCWMarket),
 target.eCWCOID = TRIM(source.eCWCOID),
 target.eCWPractice = TRIM(source.eCWPractice),
 target.eCWFacility = TRIM(source.eCWFacility),
 target.PayorGroupName = TRIM(source.PayorGroupName),
 target.PayorFinClass = TRIM(source.PayorFinClass),
 target.PayorCPID = TRIM(source.PayorCPID),
 target.PayorCactusGroup = TRIM(source.PayorCactusGroup),
 target.PayorPrimaryTimelyDays = source.PayorPrimaryTimelyDays,
 target.PayorSecondaryTimelyDays = source.PayorSecondaryTimelyDays,
 target.PayorDefFup = source.PayorDefFup,
 target.PayorCorrClm = TRIM(source.PayorCorrClm),
 target.Payor1stLevelAppealDays = source.Payor1stLevelAppealDays,
 target.Payor2ndLevelAppealDays = source.Payor2ndLevelAppealDays,
 target.PayorCapitationFlag = TRIM(source.PayorCapitationFlag),
 target.PayorInactiveFlag = TRIM(source.PayorInactiveFlag),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (OSCKey, eCWClaimNumber, AccountNumber, EncounterID, AccountPhase, StatusCode, LastPoolAccountWasInAtPoolBuild, PreviousPoolKey, LastPoolStartDate, PoolType, PoolDept, PoolDescription, PoolSubGroup, PoolFunction, PoolSLA, PayerSequenceNumber, PayerID, PayerName, BillDate, ClaimSubmissionDate, LastPoolStartAge, SubCatAccountBalance, ResponseAge, SubcategoryStatusCode, SubcategoryCode, SubcategoryDescription, SubcategorySourceDept, SubcategoryCreateDate, SubcategoryResponseDate, SubcategoryQuestionDate, ServiceDate, eCWClaimStatus, eCWClaimStatusDescription, SpecialInternalAgency, PlaceOfService, PlaceOfServiceDesc, ClaimBalance, RenderingProviderFullName, RenderingProviderNPI, eCWRegion, eCWGroup, eCWDivision, eCWMarket, eCWCOID, eCWPractice, eCWFacility, PayorGroupName, PayorFinClass, PayorCPID, PayorCactusGroup, PayorPrimaryTimelyDays, PayorSecondaryTimelyDays, PayorDefFup, PayorCorrClm, Payor1stLevelAppealDays, Payor2ndLevelAppealDays, PayorCapitationFlag, PayorInactiveFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.OSCKey, TRIM(source.eCWClaimNumber), TRIM(source.AccountNumber), TRIM(source.EncounterID), TRIM(source.AccountPhase), TRIM(source.StatusCode), TRIM(source.LastPoolAccountWasInAtPoolBuild), TRIM(source.PreviousPoolKey), source.LastPoolStartDate, TRIM(source.PoolType), TRIM(source.PoolDept), TRIM(source.PoolDescription), TRIM(source.PoolSubGroup), TRIM(source.PoolFunction), source.PoolSLA, source.PayerSequenceNumber, TRIM(source.PayerID), TRIM(source.PayerName), TRIM(source.BillDate), TRIM(source.ClaimSubmissionDate), source.LastPoolStartAge, source.SubCatAccountBalance, source.ResponseAge, TRIM(source.SubcategoryStatusCode), TRIM(source.SubcategoryCode), TRIM(source.SubcategoryDescription), TRIM(source.SubcategorySourceDept), TRIM(source.SubcategoryCreateDate), TRIM(source.SubcategoryResponseDate), TRIM(source.SubcategoryQuestionDate), TRIM(source.ServiceDate), TRIM(source.eCWClaimStatus), TRIM(source.eCWClaimStatusDescription), TRIM(source.SpecialInternalAgency), TRIM(source.PlaceOfService), TRIM(source.PlaceOfServiceDesc), source.ClaimBalance, TRIM(source.RenderingProviderFullName), TRIM(source.RenderingProviderNPI), source.eCWRegion, TRIM(source.eCWGroup), TRIM(source.eCWDivision), TRIM(source.eCWMarket), TRIM(source.eCWCOID), TRIM(source.eCWPractice), TRIM(source.eCWFacility), TRIM(source.PayorGroupName), TRIM(source.PayorFinClass), TRIM(source.PayorCPID), TRIM(source.PayorCactusGroup), source.PayorPrimaryTimelyDays, source.PayorSecondaryTimelyDays, source.PayorDefFup, TRIM(source.PayorCorrClm), source.Payor1stLevelAppealDays, source.Payor2ndLevelAppealDays, TRIM(source.PayorCapitationFlag), TRIM(source.PayorInactiveFlag), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OSCKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtOSC
      GROUP BY OSCKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtOSC');
ELSE
  COMMIT TRANSACTION;
END IF;
