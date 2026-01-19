
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtTeradataComastNBTSupplement AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtTeradataComastNBTSupplement AS source
ON target.TeradataComastNBTSupplementKey = source.TeradataComastNBTSupplementKey
WHEN MATCHED THEN
  UPDATE SET
  target.TeradataComastNBTSupplementKey = source.TeradataComastNBTSupplementKey,
 target.COID = TRIM(source.COID),
 target.COIDStatusId = source.COIDStatusId,
 target.CompanyCode = TRIM(source.CompanyCode),
 target.VPNum = TRIM(source.VPNum),
 target.GroupVicePresidentName = TRIM(source.GroupVicePresidentName),
 target.HospitalCOID = TRIM(source.HospitalCOID),
 target.HospitalUnitNum = TRIM(source.HospitalUnitNum),
 target.HospitalName = TRIM(source.HospitalName),
 target.JointVentureInd = TRIM(source.JointVentureInd),
 target.AccountLocationCode = TRIM(source.AccountLocationCode),
 target.ConsolidatorInd = TRIM(source.ConsolidatorInd),
 target.ConsolidatorId = source.ConsolidatorId,
 target.TaxId = source.TaxId,
 target.COIDStartDate = source.COIDStartDate,
 target.HBPProgramTypeId = source.HBPProgramTypeId,
 target.HBPProgramStartDate = source.HBPProgramStartDate,
 target.ARSystemCode = TRIM(source.ARSystemCode),
 target.ARSystemName = TRIM(source.ARSystemName),
 target.SameStoreInd = TRIM(source.SameStoreInd),
 target.LOBCode = TRIM(source.LOBCode),
 target.LOBSubCode = TRIM(source.LOBSubCode),
 target.TotalOperationInd = TRIM(source.TotalOperationInd),
 target.DataSourceCode = TRIM(source.DataSourceCode),
 target.SourceSystemCodeKey = TRIM(source.SourceSystemCodeKey),
 target.VicePresidentNum = TRIM(source.VicePresidentNum),
 target.TaxIdName = TRIM(source.TaxIdName),
 target.AppProgramTypeId = TRIM(source.AppProgramTypeId),
 target.ServiceLineCode = TRIM(source.ServiceLineCode),
 target.SubServiceLineCode = TRIM(source.SubServiceLineCode),
 target.VendorId = TRIM(source.VendorId),
 target.ProfitCenterCode = TRIM(source.ProfitCenterCode),
 target.SiteCode = TRIM(source.SiteCode),
 target.StartDate = source.StartDate,
 target.ActiveSw = TRIM(source.ActiveSw),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (TeradataComastNBTSupplementKey, COID, COIDStatusId, CompanyCode, VPNum, GroupVicePresidentName, HospitalCOID, HospitalUnitNum, HospitalName, JointVentureInd, AccountLocationCode, ConsolidatorInd, ConsolidatorId, TaxId, COIDStartDate, HBPProgramTypeId, HBPProgramStartDate, ARSystemCode, ARSystemName, SameStoreInd, LOBCode, LOBSubCode, TotalOperationInd, DataSourceCode, SourceSystemCodeKey, VicePresidentNum, TaxIdName, AppProgramTypeId, ServiceLineCode, SubServiceLineCode, VendorId, ProfitCenterCode, SiteCode, StartDate, ActiveSw, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.TeradataComastNBTSupplementKey, TRIM(source.COID), source.COIDStatusId, TRIM(source.CompanyCode), TRIM(source.VPNum), TRIM(source.GroupVicePresidentName), TRIM(source.HospitalCOID), TRIM(source.HospitalUnitNum), TRIM(source.HospitalName), TRIM(source.JointVentureInd), TRIM(source.AccountLocationCode), TRIM(source.ConsolidatorInd), source.ConsolidatorId, source.TaxId, source.COIDStartDate, source.HBPProgramTypeId, source.HBPProgramStartDate, TRIM(source.ARSystemCode), TRIM(source.ARSystemName), TRIM(source.SameStoreInd), TRIM(source.LOBCode), TRIM(source.LOBSubCode), TRIM(source.TotalOperationInd), TRIM(source.DataSourceCode), TRIM(source.SourceSystemCodeKey), TRIM(source.VicePresidentNum), TRIM(source.TaxIdName), TRIM(source.AppProgramTypeId), TRIM(source.ServiceLineCode), TRIM(source.SubServiceLineCode), TRIM(source.VendorId), TRIM(source.ProfitCenterCode), TRIM(source.SiteCode), source.StartDate, TRIM(source.ActiveSw), TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TeradataComastNBTSupplementKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtTeradataComastNBTSupplement
      GROUP BY TeradataComastNBTSupplementKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtTeradataComastNBTSupplement');
ELSE
  COMMIT TRANSACTION;
END IF;
