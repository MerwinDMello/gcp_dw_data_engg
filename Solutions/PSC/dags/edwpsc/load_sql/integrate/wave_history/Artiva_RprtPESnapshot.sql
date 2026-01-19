
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_RprtPESnapshot AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_RprtPESnapshot AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.COID = TRIM(source.COID),
 target.GroupDBA = TRIM(source.GroupDBA),
 target.GroupNPI = TRIM(source.GroupNPI),
 target.GroupName = TRIM(source.GroupName),
 target.GroupTaxID = TRIM(source.GroupTaxID),
 target.PPIAssociatedLocationAddress = TRIM(source.PPIAssociatedLocationAddress),
 target.PPICPIDPayorName = TRIM(source.PPICPIDPayorName),
 target.PPIClaimsAtRiskCount = source.PPIClaimsAtRiskCount,
 target.PPIClaimsOnHoldBalance = source.PPIClaimsOnHoldBalance,
 target.PPICompleteDate = source.PPICompleteDate,
 target.PPIEffectiveDate = source.PPIEffectiveDate,
 target.PPIEnrollmentAction = TRIM(source.PPIEnrollmentAction),
 target.PPIGin = TRIM(source.PPIGin),
 target.PPIInitialReceivedFromPayorDate = source.PPIInitialReceivedFromPayorDate,
 target.PPIKey = TRIM(source.PPIKey),
 target.PPILastNote = TRIM(source.PPILastNote),
 target.PPILastNoteDate = source.PPILastNoteDate,
 target.PPILeadGroupProviderStartDate = source.PPILeadGroupProviderStartDate,
 target.PPIPhase = TRIM(source.PPIPhase),
 target.PPIPin = TRIM(source.PPIPin),
 target.PPIStatus = TRIM(source.PPIStatus),
 target.PPIStatusDate = source.PPIStatusDate,
 target.PPIStatusDescription = TRIM(source.PPIStatusDescription),
 target.PPIType = TRIM(source.PPIType),
 target.PPIVoidFlag = TRIM(source.PPIVoidFlag),
 target.PayorSplitByLocationFlag = TRIM(source.PayorSplitByLocationFlag),
 target.PracticeBillingAddress = TRIM(source.PracticeBillingAddress),
 target.ProviderAddressType = TRIM(source.ProviderAddressType),
 target.ProviderFullName = TRIM(source.ProviderFullName),
 target.ProviderPrimarySpecialty = TRIM(source.ProviderPrimarySpecialty),
 target.ProviderSSPActiveFlag = TRIM(source.ProviderSSPActiveFlag),
 target.ProviderTaxonomyCode = TRIM(source.ProviderTaxonomyCode),
 target.ProviderTerminationDate = source.ProviderTerminationDate,
 target.ProviderTerminationFlag = TRIM(source.ProviderTerminationFlag),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ArtivaPPIKey = source.ArtivaPPIKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, COID, GroupDBA, GroupNPI, GroupName, GroupTaxID, PPIAssociatedLocationAddress, PPICPIDPayorName, PPIClaimsAtRiskCount, PPIClaimsOnHoldBalance, PPICompleteDate, PPIEffectiveDate, PPIEnrollmentAction, PPIGin, PPIInitialReceivedFromPayorDate, PPIKey, PPILastNote, PPILastNoteDate, PPILeadGroupProviderStartDate, PPIPhase, PPIPin, PPIStatus, PPIStatusDate, PPIStatusDescription, PPIType, PPIVoidFlag, PayorSplitByLocationFlag, PracticeBillingAddress, ProviderAddressType, ProviderFullName, ProviderPrimarySpecialty, ProviderSSPActiveFlag, ProviderTaxonomyCode, ProviderTerminationDate, ProviderTerminationFlag, ProviderNPI, ArtivaPPIKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, TRIM(source.COID), TRIM(source.GroupDBA), TRIM(source.GroupNPI), TRIM(source.GroupName), TRIM(source.GroupTaxID), TRIM(source.PPIAssociatedLocationAddress), TRIM(source.PPICPIDPayorName), source.PPIClaimsAtRiskCount, source.PPIClaimsOnHoldBalance, source.PPICompleteDate, source.PPIEffectiveDate, TRIM(source.PPIEnrollmentAction), TRIM(source.PPIGin), source.PPIInitialReceivedFromPayorDate, TRIM(source.PPIKey), TRIM(source.PPILastNote), source.PPILastNoteDate, source.PPILeadGroupProviderStartDate, TRIM(source.PPIPhase), TRIM(source.PPIPin), TRIM(source.PPIStatus), source.PPIStatusDate, TRIM(source.PPIStatusDescription), TRIM(source.PPIType), TRIM(source.PPIVoidFlag), TRIM(source.PayorSplitByLocationFlag), TRIM(source.PracticeBillingAddress), TRIM(source.ProviderAddressType), TRIM(source.ProviderFullName), TRIM(source.ProviderPrimarySpecialty), TRIM(source.ProviderSSPActiveFlag), TRIM(source.ProviderTaxonomyCode), source.ProviderTerminationDate, TRIM(source.ProviderTerminationFlag), TRIM(source.ProviderNPI), source.ArtivaPPIKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_RprtPESnapshot
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_RprtPESnapshot');
ELSE
  COMMIT TRANSACTION;
END IF;
