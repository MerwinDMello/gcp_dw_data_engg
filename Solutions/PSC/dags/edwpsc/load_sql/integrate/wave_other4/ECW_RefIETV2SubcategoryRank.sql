
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefIETV2SubcategoryRank AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefIETV2SubcategoryRank AS source
ON target.SubcategoryRankKey = source.SubcategoryRankKey
WHEN MATCHED THEN
  UPDATE SET
  target.SubcategoryRankKey = source.SubcategoryRankKey,
 target.SubcategoryID = source.SubcategoryID,
 target.ErrorType = TRIM(source.ErrorType),
 target.ErrorTypeDescription = TRIM(source.ErrorTypeDescription),
 target.SubcategoryName = TRIM(source.SubcategoryName),
 target.IETV2SubcategoryRank = source.IETV2SubcategoryRank,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SubcategoryRankKey, SubcategoryID, ErrorType, ErrorTypeDescription, SubcategoryName, IETV2SubcategoryRank, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SubcategoryRankKey, source.SubcategoryID, TRIM(source.ErrorType), TRIM(source.ErrorTypeDescription), TRIM(source.SubcategoryName), source.IETV2SubcategoryRank, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SubcategoryRankKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefIETV2SubcategoryRank
      GROUP BY SubcategoryRankKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefIETV2SubcategoryRank');
ELSE
  COMMIT TRANSACTION;
END IF;
