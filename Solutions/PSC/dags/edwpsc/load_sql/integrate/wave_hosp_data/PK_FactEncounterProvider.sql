
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PK_FactEncounterProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.PK_FactEncounterProvider AS source
ON target.PKEncounterProviderKey = source.PKEncounterProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.PKEncounterProviderKey = source.PKEncounterProviderKey,
 target.PKRegionName = TRIM(source.PKRegionName),
 target.PKEncounterKey = source.PKEncounterKey,
 target.EncounterId = source.EncounterId,
 target.BeginEffectiveDate = source.BeginEffectiveDate,
 target.EndEffectiveDate = source.EndEffectiveDate,
 target.ProviderType = TRIM(source.ProviderType),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.ProviderMiddleName = TRIM(source.ProviderMiddleName),
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.PKFinancialNumber = TRIM(source.PKFinancialNumber),
 target.PersonId = source.PersonId
WHEN NOT MATCHED THEN
  INSERT (PKEncounterProviderKey, PKRegionName, PKEncounterKey, EncounterId, BeginEffectiveDate, EndEffectiveDate, ProviderType, ProviderLastName, ProviderFirstName, ProviderMiddleName, ProviderNPI, DeleteFlag, DWLastUpdateDateTime, SourceAPrimaryKeyValue, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, PKFinancialNumber, PersonId)
  VALUES (source.PKEncounterProviderKey, TRIM(source.PKRegionName), source.PKEncounterKey, source.EncounterId, source.BeginEffectiveDate, source.EndEffectiveDate, TRIM(source.ProviderType), TRIM(source.ProviderLastName), TRIM(source.ProviderFirstName), TRIM(source.ProviderMiddleName), TRIM(source.ProviderNPI), source.DeleteFlag, source.DWLastUpdateDateTime, source.SourceAPrimaryKeyValue, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.PKFinancialNumber), source.PersonId);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PKEncounterProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.PK_FactEncounterProvider
      GROUP BY PKEncounterProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PK_FactEncounterProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
