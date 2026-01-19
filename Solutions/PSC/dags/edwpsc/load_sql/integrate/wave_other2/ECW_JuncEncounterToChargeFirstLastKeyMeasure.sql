
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncEncounterToChargeFirstLastKeyMeasure AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncEncounterToChargeFirstLastKeyMeasure AS source
ON target.EncounterToChargeKey = source.EncounterToChargeKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncounterToChargeKey = source.EncounterToChargeKey,
 target.LastSourceSystem = TRIM(source.LastSourceSystem),
 target.LastSourceSystemDTM = source.LastSourceSystemDTM,
 target.LastRegionID = source.LastRegionID,
 target.LastStatus = TRIM(source.LastStatus),
 target.LastStatusDTM = source.LastStatusDTM,
 target.LastOwner = TRIM(source.LastOwner),
 target.ClaimCount = source.ClaimCount,
 target.EncounterCPTCount = source.EncounterCPTCount,
 target.ClaimCPTCount = source.ClaimCPTCount,
 target.ChargeQuantity = source.ChargeQuantity,
 target.PSCProviderFlag = source.PSCProviderFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (EncounterToChargeKey, LastSourceSystem, LastSourceSystemDTM, LastRegionID, LastStatus, LastStatusDTM, LastOwner, ClaimCount, EncounterCPTCount, ClaimCPTCount, ChargeQuantity, PSCProviderFlag, DWLastUpdateDateTime)
  VALUES (source.EncounterToChargeKey, TRIM(source.LastSourceSystem), source.LastSourceSystemDTM, source.LastRegionID, TRIM(source.LastStatus), source.LastStatusDTM, TRIM(source.LastOwner), source.ClaimCount, source.EncounterCPTCount, source.ClaimCPTCount, source.ChargeQuantity, source.PSCProviderFlag, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EncounterToChargeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncEncounterToChargeFirstLastKeyMeasure
      GROUP BY EncounterToChargeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncEncounterToChargeFirstLastKeyMeasure');
ELSE
  COMMIT TRANSACTION;
END IF;
