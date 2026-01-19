
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncEnterpriseDirectory AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncEnterpriseDirectory AS source
ON target.ECWEnterpriseDirectoryKey = source.ECWEnterpriseDirectoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.ECWEnterpriseDirectoryKey = source.ECWEnterpriseDirectoryKey,
 target.RegionKey = source.RegionKey,
 target.OrgId = source.OrgId,
 target.ParentId = source.ParentId,
 target.OrgName = TRIM(source.OrgName),
 target.OrgDesc = TRIM(source.OrgDesc),
 target.OrgType = TRIM(source.OrgType),
 target.DeleteFlag = source.DeleteFlag,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ECWEnterpriseDirectoryKey, RegionKey, OrgId, ParentId, OrgName, OrgDesc, OrgType, DeleteFlag, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ECWEnterpriseDirectoryKey, source.RegionKey, source.OrgId, source.ParentId, TRIM(source.OrgName), TRIM(source.OrgDesc), TRIM(source.OrgType), source.DeleteFlag, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ECWEnterpriseDirectoryKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncEnterpriseDirectory
      GROUP BY ECWEnterpriseDirectoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncEnterpriseDirectory');
ELSE
  COMMIT TRANSACTION;
END IF;
