
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimLineCharge AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimLineCharge AS source
ON target.ClaimLineChargeKey = source.ClaimLineChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.TransactionNumber = source.TransactionNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.FacilityKey = source.FacilityKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.ClaimLineLiabilityIplanKey = source.ClaimLineLiabilityIplanKey,
 target.CPTCodeKey = source.CPTCodeKey,
 target.CPTCode = TRIM(source.CPTCode),
 target.CPTOrder = source.CPTOrder,
 target.CPTStartServiceDateKey = source.CPTStartServiceDateKey,
 target.CPTEndServiceDateKey = source.CPTEndServiceDateKey,
 target.CPTPOSKey = source.CPTPOSKey,
 target.CPTTypeOfServiceKey = source.CPTTypeOfServiceKey,
 target.CPTCharges = source.CPTCharges,
 target.CPTChargesPerUnit = source.CPTChargesPerUnit,
 target.CPTUnits = source.CPTUnits,
 target.CPTAllowed = source.CPTAllowed,
 target.CPTAllowedPerUnit = source.CPTAllowedPerUnit,
 target.CPTPatientPortion = source.CPTPatientPortion,
 target.CPTTotalPatientPortion = source.CPTTotalPatientPortion,
 target.CPTNonCoveredCharges = source.CPTNonCoveredCharges,
 target.CPTBalance = source.CPTBalance,
 target.CPTCalculatedBalance = source.CPTCalculatedBalance,
 target.CPTModifier1 = TRIM(source.CPTModifier1),
 target.CPTModifier2 = TRIM(source.CPTModifier2),
 target.CPTModifier3 = TRIM(source.CPTModifier3),
 target.CPTModifier4 = TRIM(source.CPTModifier4),
 target.CPTBillToIns = source.CPTBillToIns,
 target.CPTBillToPt = source.CPTBillToPt,
 target.ICD1 = TRIM(source.ICD1),
 target.ICD2 = TRIM(source.ICD2),
 target.ICD3 = TRIM(source.ICD3),
 target.ICD4 = TRIM(source.ICD4),
 target.CliaID = TRIM(source.CliaID),
 target.NDCCode = TRIM(source.NDCCode),
 target.NDCUnits = TRIM(source.NDCUnits),
 target.NDCUnitPrice = source.NDCUnitPrice,
 target.NDCUom = TRIM(source.NDCUom),
 target.CPTDeleteFlag = source.CPTDeleteFlag,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.EtrID = source.EtrID
WHEN NOT MATCHED THEN
  INSERT (ClaimLineChargeKey, ClaimKey, ClaimNumber, TransactionNumber, VisitNumber, RegionKey, Coid, CoidConfigurationKey, FacilityKey, ServicingProviderKey, ClaimPayer1IplanKey, ClaimLineLiabilityIplanKey, CPTCodeKey, CPTCode, CPTOrder, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTPOSKey, CPTTypeOfServiceKey, CPTCharges, CPTChargesPerUnit, CPTUnits, CPTAllowed, CPTAllowedPerUnit, CPTPatientPortion, CPTTotalPatientPortion, CPTNonCoveredCharges, CPTBalance, CPTCalculatedBalance, CPTModifier1, CPTModifier2, CPTModifier3, CPTModifier4, CPTBillToIns, CPTBillToPt, ICD1, ICD2, ICD3, ICD4, CliaID, NDCCode, NDCUnits, NDCUnitPrice, NDCUom, CPTDeleteFlag, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, EtrID)
  VALUES (source.ClaimLineChargeKey, source.ClaimKey, source.ClaimNumber, source.TransactionNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.FacilityKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.ClaimLineLiabilityIplanKey, source.CPTCodeKey, TRIM(source.CPTCode), source.CPTOrder, source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, source.CPTPOSKey, source.CPTTypeOfServiceKey, source.CPTCharges, source.CPTChargesPerUnit, source.CPTUnits, source.CPTAllowed, source.CPTAllowedPerUnit, source.CPTPatientPortion, source.CPTTotalPatientPortion, source.CPTNonCoveredCharges, source.CPTBalance, source.CPTCalculatedBalance, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), TRIM(source.CPTModifier3), TRIM(source.CPTModifier4), source.CPTBillToIns, source.CPTBillToPt, TRIM(source.ICD1), TRIM(source.ICD2), TRIM(source.ICD3), TRIM(source.ICD4), TRIM(source.CliaID), TRIM(source.NDCCode), TRIM(source.NDCUnits), source.NDCUnitPrice, TRIM(source.NDCUom), source.CPTDeleteFlag, source.SourceAPrimaryKeyValue, source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.EtrID);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLineChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimLineCharge
      GROUP BY ClaimLineChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimLineCharge');
ELSE
  COMMIT TRANSACTION;
END IF;
