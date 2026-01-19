
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.vw_PV_FactClaimLineCharge AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimLineCharge AS source
ON target.ClaimLineChargeKey = source.ClaimLineChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.CPTCodeKey = source.CPTCodeKey,
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
 target.CPTDeleteFlag = source.CPTDeleteFlag,
 target.CPTOrder = source.CPTOrder,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ICD1 = TRIM(source.ICD1),
 target.ICD1Code = TRIM(source.ICD1Code),
 target.ICD2 = TRIM(source.ICD2),
 target.ICD2Code = TRIM(source.ICD2Code),
 target.ICD3 = TRIM(source.ICD3),
 target.ICD3Code = TRIM(source.ICD3Code),
 target.ICD4 = TRIM(source.ICD4),
 target.ICD4Code = TRIM(source.ICD4Code),
 target.CliaID = TRIM(source.CliaID),
 target.NDCCode = TRIM(source.NDCCode),
 target.NDCUnits = TRIM(source.NDCUnits),
 target.NDCUnitPrice = source.NDCUnitPrice,
 target.NDCUom = TRIM(source.NDCUom),
 target.CPTCode = TRIM(source.CPTCode),
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.PracticeName = TRIM(source.PracticeName),
 target.NDCSentOnBillFlag = TRIM(source.NDCSentOnBillFlag),
 target.EMCodeFlag = source.EMCodeFlag
WHEN NOT MATCHED THEN
  INSERT (ClaimLineChargeKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, CPTCodeKey, CPTStartServiceDateKey, CPTEndServiceDateKey, CPTPOSKey, CPTTypeOfServiceKey, CPTCharges, CPTChargesPerUnit, CPTUnits, CPTAllowed, CPTAllowedPerUnit, CPTPatientPortion, CPTTotalPatientPortion, CPTNonCoveredCharges, CPTBalance, CPTCalculatedBalance, CPTModifier1, CPTModifier2, CPTModifier3, CPTModifier4, CPTBillToIns, CPTBillToPt, CPTDeleteFlag, CPTOrder, SourceAPrimaryKeyValue, SourceARecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ICD1, ICD1Code, ICD2, ICD2Code, ICD3, ICD3Code, ICD4, ICD4Code, CliaID, NDCCode, NDCUnits, NDCUnitPrice, NDCUom, CPTCode, SourceBPrimaryKeyValue, PracticeName, NDCSentOnBillFlag, EMCodeFlag)
  VALUES (source.ClaimLineChargeKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.CPTCodeKey, source.CPTStartServiceDateKey, source.CPTEndServiceDateKey, source.CPTPOSKey, source.CPTTypeOfServiceKey, source.CPTCharges, source.CPTChargesPerUnit, source.CPTUnits, source.CPTAllowed, source.CPTAllowedPerUnit, source.CPTPatientPortion, source.CPTTotalPatientPortion, source.CPTNonCoveredCharges, source.CPTBalance, source.CPTCalculatedBalance, TRIM(source.CPTModifier1), TRIM(source.CPTModifier2), TRIM(source.CPTModifier3), TRIM(source.CPTModifier4), source.CPTBillToIns, source.CPTBillToPt, source.CPTDeleteFlag, source.CPTOrder, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ICD1), TRIM(source.ICD1Code), TRIM(source.ICD2), TRIM(source.ICD2Code), TRIM(source.ICD3), TRIM(source.ICD3Code), TRIM(source.ICD4), TRIM(source.ICD4Code), TRIM(source.CliaID), TRIM(source.NDCCode), TRIM(source.NDCUnits), source.NDCUnitPrice, TRIM(source.NDCUom), TRIM(source.CPTCode), TRIM(source.SourceBPrimaryKeyValue), TRIM(source.PracticeName), TRIM(source.NDCSentOnBillFlag), source.EMCodeFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLineChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.vw_PV_FactClaimLineCharge
      GROUP BY ClaimLineChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.vw_PV_FactClaimLineCharge');
ELSE
  COMMIT TRANSACTION;
END IF;
