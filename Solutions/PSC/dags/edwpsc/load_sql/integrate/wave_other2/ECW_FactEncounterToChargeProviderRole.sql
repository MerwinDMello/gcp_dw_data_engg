
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeProviderRole AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactEncounterToChargeProviderRole AS source
ON target.EncounterToChargeProviderRole = source.EncounterToChargeProviderRole
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterToChargeProviderRole = source.EncounterToChargeProviderRole,
 target.EncounterToChargeKey = source.EncounterToChargeKey,
 target.ProviderRole = TRIM(source.ProviderRole),
 target.ProviderName = TRIM(source.ProviderName),
 target.FacilityMnemonic = TRIM(source.FacilityMnemonic),
 target.ProviderMnemonic = TRIM(source.ProviderMnemonic),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderDEALicenseNumber = TRIM(source.ProviderDEALicenseNumber),
 target.PSCProviderFlag = source.PSCProviderFlag,
 target.AssignedDTM = source.AssignedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (EncounterToChargeProviderRole, EncounterToChargeKey, ProviderRole, ProviderName, FacilityMnemonic, ProviderMnemonic, ProviderNPI, ProviderDEALicenseNumber, PSCProviderFlag, AssignedDTM, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.EncounterToChargeProviderRole, source.EncounterToChargeKey, TRIM(source.ProviderRole), TRIM(source.ProviderName), TRIM(source.FacilityMnemonic), TRIM(source.ProviderMnemonic), TRIM(source.ProviderNPI), TRIM(source.ProviderDEALicenseNumber), source.PSCProviderFlag, source.AssignedDTM, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterToChargeProviderRole
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeProviderRole
      GROUP BY EncounterToChargeProviderRole
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactEncounterToChargeProviderRole');
ELSE
  COMMIT TRANSACTION;
END IF;
