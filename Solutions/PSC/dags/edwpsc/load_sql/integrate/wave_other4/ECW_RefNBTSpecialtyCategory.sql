
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefNBTSpecialtyCategory AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefNBTSpecialtyCategory AS source
ON target.NBTSpecialtyCategoryIdKey = source.NBTSpecialtyCategoryIdKey
WHEN MATCHED THEN
  UPDATE SET
  target.NBTSpecialtyCategoryIdKey = source.NBTSpecialtyCategoryIdKey,
 target.NBTSpecialtyCategoryId = source.NBTSpecialtyCategoryId,
 target.NBTSpecialtyCategoryDesc = TRIM(source.NBTSpecialtyCategoryDesc),
 target.Active = source.Active,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (NBTSpecialtyCategoryIdKey, NBTSpecialtyCategoryId, NBTSpecialtyCategoryDesc, Active, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.NBTSpecialtyCategoryIdKey, source.NBTSpecialtyCategoryId, TRIM(source.NBTSpecialtyCategoryDesc), source.Active, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NBTSpecialtyCategoryIdKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefNBTSpecialtyCategory
      GROUP BY NBTSpecialtyCategoryIdKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefNBTSpecialtyCategory');
ELSE
  COMMIT TRANSACTION;
END IF;
