
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefCoidStar AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefCoidStar AS source
ON target.CoidStarKey = source.CoidStarKey
WHEN MATCHED THEN
  UPDATE SET
  target.CoidStarKey = source.CoidStarKey,
 target.Coid = TRIM(source.Coid),
 target.CoidName = TRIM(source.CoidName),
 target.CoidSpecialty = TRIM(source.CoidSpecialty),
 target.CoidCompanyCode = TRIM(source.CoidCompanyCode),
 target.CoidUnitNumber = TRIM(source.CoidUnitNumber),
 target.PscRevCycleMgmt = TRIM(source.PscRevCycleMgmt),
 target.GroupKey = source.GroupKey,
 target.GroupName = TRIM(source.GroupName),
 target.DivisionKey = source.DivisionKey,
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketKey = source.MarketKey,
 target.MarketName = TRIM(source.MarketName),
 target.StateKey = TRIM(source.StateKey),
 target.HCAPS300 = TRIM(source.HCAPS300),
 target.CenterKey = source.CenterKey,
 target.CenterDescription = TRIM(source.CenterDescription),
 target.ServiceLine = TRIM(source.ServiceLine),
 target.CoidLOB = TRIM(source.CoidLOB),
 target.CoidSubLOB = TRIM(source.CoidSubLOB),
 target.LOBName = TRIM(source.LOBName),
 target.SubLOBName = TRIM(source.SubLOBName),
 target.TraumaBurnFlag = TRIM(source.TraumaBurnFlag),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.COIDWithLeadingZero = TRIM(source.COIDWithLeadingZero),
 target.DeleteFlag = TRIM(source.DeleteFlag),
 target.DivisionCode = TRIM(source.DivisionCode),
 target.GroupCode = TRIM(source.GroupCode),
 target.HospitalCOID = TRIM(source.HospitalCOID),
 target.HospitalName = TRIM(source.HospitalName),
 target.MarketCode = TRIM(source.MarketCode),
 target.PPMSFlag = source.PPMSFlag
WHEN NOT MATCHED THEN
  INSERT (CoidStarKey, Coid, CoidName, CoidSpecialty, CoidCompanyCode, CoidUnitNumber, PscRevCycleMgmt, GroupKey, GroupName, DivisionKey, DivisionName, MarketKey, MarketName, StateKey, HCAPS300, CenterKey, CenterDescription, ServiceLine, CoidLOB, CoidSubLOB, LOBName, SubLOBName, TraumaBurnFlag, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, COIDWithLeadingZero, DeleteFlag, DivisionCode, GroupCode, HospitalCOID, HospitalName, MarketCode, PPMSFlag)
  VALUES (source.CoidStarKey, TRIM(source.Coid), TRIM(source.CoidName), TRIM(source.CoidSpecialty), TRIM(source.CoidCompanyCode), TRIM(source.CoidUnitNumber), TRIM(source.PscRevCycleMgmt), source.GroupKey, TRIM(source.GroupName), source.DivisionKey, TRIM(source.DivisionName), source.MarketKey, TRIM(source.MarketName), TRIM(source.StateKey), TRIM(source.HCAPS300), source.CenterKey, TRIM(source.CenterDescription), TRIM(source.ServiceLine), TRIM(source.CoidLOB), TRIM(source.CoidSubLOB), TRIM(source.LOBName), TRIM(source.SubLOBName), TRIM(source.TraumaBurnFlag), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.COIDWithLeadingZero), TRIM(source.DeleteFlag), TRIM(source.DivisionCode), TRIM(source.GroupCode), TRIM(source.HospitalCOID), TRIM(source.HospitalName), TRIM(source.MarketCode), source.PPMSFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CoidStarKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefCoidStar
      GROUP BY CoidStarKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefCoidStar');
ELSE
  COMMIT TRANSACTION;
END IF;
