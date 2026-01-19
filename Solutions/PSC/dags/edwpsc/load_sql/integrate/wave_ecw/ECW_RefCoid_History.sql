
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefCoid_History AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefCoid_History AS source
ON target.Coid = source.Coid AND target.SysStartTime = source.SysStartTime AND target.SysEndTime = source.SysEndTime
WHEN MATCHED THEN
  UPDATE SET
  target.Coid = TRIM(source.Coid),
 target.CoidName = TRIM(source.CoidName),
 target.StateKey = TRIM(source.StateKey),
 target.CoidConsolidationIndicator = TRIM(source.CoidConsolidationIndicator),
 target.CoidCompanyCode = TRIM(source.CoidCompanyCode),
 target.CoidUnitNumber = TRIM(source.CoidUnitNumber),
 target.CoidLOB = TRIM(source.CoidLOB),
 target.CoidSubLOB = TRIM(source.CoidSubLOB),
 target.MarketKey = source.MarketKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.LOBName = TRIM(source.LOBName),
 target.SubLOBName = TRIM(source.SubLOBName),
 target.DeleteFlag = source.DeleteFlag,
 target.coidstatflag = source.coidstatflag,
 target.PPMSFlag = source.PPMSFlag,
 target.CenterKey = source.CenterKey,
 target.CoidSpecialty = TRIM(source.CoidSpecialty),
 target.CoidSameStoreCode = TRIM(source.CoidSameStoreCode),
 target.CoidSameStoreFlag = source.CoidSameStoreFlag,
 target.PscRevCycleMgmt = TRIM(source.PscRevCycleMgmt),
 target.SysStartTime = source.SysStartTime,
 target.SysEndTime = source.SysEndTime,
 target.COIDWithLeadingZero = TRIM(source.COIDWithLeadingZero)
WHEN NOT MATCHED THEN
  INSERT (Coid, CoidName, StateKey, CoidConsolidationIndicator, CoidCompanyCode, CoidUnitNumber, CoidLOB, CoidSubLOB, MarketKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, LOBName, SubLOBName, DeleteFlag, coidstatflag, PPMSFlag, CenterKey, CoidSpecialty, CoidSameStoreCode, CoidSameStoreFlag, PscRevCycleMgmt, SysStartTime, SysEndTime, COIDWithLeadingZero)
  VALUES (TRIM(source.Coid), TRIM(source.CoidName), TRIM(source.StateKey), TRIM(source.CoidConsolidationIndicator), TRIM(source.CoidCompanyCode), TRIM(source.CoidUnitNumber), TRIM(source.CoidLOB), TRIM(source.CoidSubLOB), source.MarketKey, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.LOBName), TRIM(source.SubLOBName), source.DeleteFlag, source.coidstatflag, source.PPMSFlag, source.CenterKey, TRIM(source.CoidSpecialty), TRIM(source.CoidSameStoreCode), source.CoidSameStoreFlag, TRIM(source.PscRevCycleMgmt), source.SysStartTime, source.SysEndTime, TRIM(source.COIDWithLeadingZero));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT Coid, SysStartTime, SysEndTime
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefCoid_History
      GROUP BY Coid, SysStartTime, SysEndTime
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefCoid_History');
ELSE
  COMMIT TRANSACTION;
END IF;
