
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactKronos AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactKronos AS source
ON target.KronosKey = source.KronosKey
WHEN MATCHED THEN
  UPDATE SET
  target.KronosKey = source.KronosKey,
 target.EmployeeDailyTimeLogSK = source.EmployeeDailyTimeLogSK,
 target.Coid = TRIM(source.Coid),
 target.CompanyCode = TRIM(source.CompanyCode),
 target.EmployeeSK = source.EmployeeSK,
 target.VendorEmployeeSrcSysKey = source.VendorEmployeeSrcSysKey,
 target.CompanyId = source.CompanyId,
 target.Employee34Id = TRIM(source.Employee34Id),
 target.NPI = TRIM(source.NPI),
 target.DeptCode = TRIM(source.DeptCode),
 target.JobCode = TRIM(source.JobCode),
 target.EmployeeActualLogInDateTime = source.EmployeeActualLogInDateTime,
 target.EmployeeActualLogOutDateTime = source.EmployeeActualLogOutDateTime,
 target.EmployeeActualElapsedTimeCnt = source.EmployeeActualElapsedTimeCnt,
 target.EmployeeRoundedLogInDateTime = source.EmployeeRoundedLogInDateTime,
 target.EmployeeRoundedLogOutDateTime = source.EmployeeRoundedLogOutDateTime,
 target.EmployeeRoundedElapsedTimeCnt = source.EmployeeRoundedElapsedTimeCnt,
 target.PaySmryGroupCode = TRIM(source.PaySmryGroupCode),
 target.PayCodeHourCnt = source.PayCodeHourCnt,
 target.DataServerCode = TRIM(source.DataServerCode),
 target.SystemCode = TRIM(source.SystemCode),
 target.SourceLastUpdateDateTime = source.SourceLastUpdateDateTime,
 target.DeptDesc = TRIM(source.DeptDesc),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (KronosKey, EmployeeDailyTimeLogSK, Coid, CompanyCode, EmployeeSK, VendorEmployeeSrcSysKey, CompanyId, Employee34Id, NPI, DeptCode, JobCode, EmployeeActualLogInDateTime, EmployeeActualLogOutDateTime, EmployeeActualElapsedTimeCnt, EmployeeRoundedLogInDateTime, EmployeeRoundedLogOutDateTime, EmployeeRoundedElapsedTimeCnt, PaySmryGroupCode, PayCodeHourCnt, DataServerCode, SystemCode, SourceLastUpdateDateTime, DeptDesc, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.KronosKey, source.EmployeeDailyTimeLogSK, TRIM(source.Coid), TRIM(source.CompanyCode), source.EmployeeSK, source.VendorEmployeeSrcSysKey, source.CompanyId, TRIM(source.Employee34Id), TRIM(source.NPI), TRIM(source.DeptCode), TRIM(source.JobCode), source.EmployeeActualLogInDateTime, source.EmployeeActualLogOutDateTime, source.EmployeeActualElapsedTimeCnt, source.EmployeeRoundedLogInDateTime, source.EmployeeRoundedLogOutDateTime, source.EmployeeRoundedElapsedTimeCnt, TRIM(source.PaySmryGroupCode), source.PayCodeHourCnt, TRIM(source.DataServerCode), TRIM(source.SystemCode), source.SourceLastUpdateDateTime, TRIM(source.DeptDesc), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT KronosKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactKronos
      GROUP BY KronosKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactKronos');
ELSE
  COMMIT TRANSACTION;
END IF;
