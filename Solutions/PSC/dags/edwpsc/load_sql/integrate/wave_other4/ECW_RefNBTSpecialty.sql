
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefNBTSpecialty AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefNBTSpecialty AS source
ON target.NBTSpecialtyIdKey = source.NBTSpecialtyIdKey
WHEN MATCHED THEN
  UPDATE SET
  target.NBTSpecialtyIdKey = source.NBTSpecialtyIdKey,
 target.SpecialtyId = source.SpecialtyId,
 target.SpecialtyCode = TRIM(source.SpecialtyCode),
 target.SpecialtyDescription = TRIM(source.SpecialtyDescription),
 target.SpecialtyTypeId = source.SpecialtyTypeId,
 target.SpecialtyActive = source.SpecialtyActive,
 target.SpecialtyUUID = TRIM(source.SpecialtyUUID),
 target.NBTSpecialtyCategoryIdKey = source.NBTSpecialtyCategoryIdKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (NBTSpecialtyIdKey, SpecialtyId, SpecialtyCode, SpecialtyDescription, SpecialtyTypeId, SpecialtyActive, SpecialtyUUID, NBTSpecialtyCategoryIdKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.NBTSpecialtyIdKey, source.SpecialtyId, TRIM(source.SpecialtyCode), TRIM(source.SpecialtyDescription), source.SpecialtyTypeId, source.SpecialtyActive, TRIM(source.SpecialtyUUID), source.NBTSpecialtyCategoryIdKey, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NBTSpecialtyIdKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefNBTSpecialty
      GROUP BY NBTSpecialtyIdKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefNBTSpecialty');
ELSE
  COMMIT TRANSACTION;
END IF;
