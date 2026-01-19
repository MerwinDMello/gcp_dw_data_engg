
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTICCAppFacilityList AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTICCAppFacilityList AS source
ON target.FacilityListKey = source.FacilityListKey
WHEN MATCHED THEN
  UPDATE SET
  target.FacilityListKey = source.FacilityListKey,
 target.UniqueIdentifier = source.UniqueIdentifier,
 target.ContractControlNumber = source.ContractControlNumber,
 target.AssignedVendor = TRIM(source.AssignedVendor),
 target.InventoryType = TRIM(source.InventoryType),
 target.COID = source.COID,
 target.RegionKey = source.RegionKey,
 target.SiteID = source.SiteID,
 target.FacilityName = TRIM(source.FacilityName),
 target.PoolExecutiveName = TRIM(source.PoolExecutiveName),
 target.PoolDirectorName = TRIM(source.PoolDirectorName),
 target.PoolManagerName = TRIM(source.PoolManagerName),
 target.EffectiveDate = source.EffectiveDate,
 target.TermedDate = source.TermedDate,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (FacilityListKey, UniqueIdentifier, ContractControlNumber, AssignedVendor, InventoryType, COID, RegionKey, SiteID, FacilityName, PoolExecutiveName, PoolDirectorName, PoolManagerName, EffectiveDate, TermedDate, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.FacilityListKey, source.UniqueIdentifier, source.ContractControlNumber, TRIM(source.AssignedVendor), TRIM(source.InventoryType), source.COID, source.RegionKey, source.SiteID, TRIM(source.FacilityName), TRIM(source.PoolExecutiveName), TRIM(source.PoolDirectorName), TRIM(source.PoolManagerName), source.EffectiveDate, source.TermedDate, source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT FacilityListKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTICCAppFacilityList
      GROUP BY FacilityListKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTICCAppFacilityList');
ELSE
  COMMIT TRANSACTION;
END IF;
