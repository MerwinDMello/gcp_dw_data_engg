
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactRH277Response AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactRH277Response AS source
ON target.RH277ResponseKey = source.RH277ResponseKey
WHEN MATCHED THEN
  UPDATE SET
  target.RH277ResponseKey = source.RH277ResponseKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.ImportDateKey = source.ImportDateKey,
 target.RH277ResponseClaimID = TRIM(source.RH277ResponseClaimID),
 target.RH277ResponsePatientControlNbr = TRIM(source.RH277ResponsePatientControlNbr),
 target.RH277ResponseDateImportedKey = source.RH277ResponseDateImportedKey,
 target.RH277ResponseRespondingPayer = TRIM(source.RH277ResponseRespondingPayer),
 target.RH277ResponseStatusCtgyCode = TRIM(source.RH277ResponseStatusCtgyCode),
 target.RH277ResponseStatusCode = TRIM(source.RH277ResponseStatusCode),
 target.RH277FileName = TRIM(source.RH277FileName),
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (RH277ResponseKey, ClaimKey, ClaimNumber, RegionKey, Coid, ImportDateKey, RH277ResponseClaimID, RH277ResponsePatientControlNbr, RH277ResponseDateImportedKey, RH277ResponseRespondingPayer, RH277ResponseStatusCtgyCode, RH277ResponseStatusCode, RH277FileName, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.RH277ResponseKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.ImportDateKey, TRIM(source.RH277ResponseClaimID), TRIM(source.RH277ResponsePatientControlNbr), source.RH277ResponseDateImportedKey, TRIM(source.RH277ResponseRespondingPayer), TRIM(source.RH277ResponseStatusCtgyCode), TRIM(source.RH277ResponseStatusCode), TRIM(source.RH277FileName), TRIM(source.SourcePrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT RH277ResponseKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactRH277Response
      GROUP BY RH277ResponseKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactRH277Response');
ELSE
  COMMIT TRANSACTION;
END IF;
