
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactUserProductivity AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactUserProductivity AS source
ON target.LoadKey = source.LoadKey
WHEN MATCHED THEN
  UPDATE SET
  target.LoadKey = source.LoadKey,
 target.LoadDateKey = source.LoadDateKey,
 target.UserProductivityKey = source.UserProductivityKey,
 target.BSUID = source.BSUID,
 target.ProcessID = source.ProcessID,
 target.ControllNumber = source.ControllNumber,
 target.EmployeeGroupName = TRIM(source.EmployeeGroupName),
 target.ActionDate = source.ActionDate,
 target.ActionUserID = TRIM(source.ActionUserID),
 target.ClaimKey = source.ClaimKey,
 target.InvoiceNumber = TRIM(source.InvoiceNumber),
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.GuarantorID = source.GuarantorID,
 target.AccountNumber = TRIM(source.AccountNumber),
 target.PRPNumber = source.PRPNumber,
 target.PPIKey = TRIM(source.PPIKey),
 target.DocumentHandle = source.DocumentHandle,
 target.BatchId = TRIM(source.BatchId),
 target.SourceSystem = TRIM(source.SourceSystem),
 target.Pool = TRIM(source.Pool),
 target.PoolFunction = TRIM(source.PoolFunction),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (LoadKey, LoadDateKey, UserProductivityKey, BSUID, ProcessID, ControllNumber, EmployeeGroupName, ActionDate, ActionUserID, ClaimKey, InvoiceNumber, Coid, RegionKey, GuarantorID, AccountNumber, PRPNumber, PPIKey, DocumentHandle, BatchId, SourceSystem, Pool, PoolFunction, DWLastUpdateDateTime)
  VALUES (source.LoadKey, source.LoadDateKey, source.UserProductivityKey, source.BSUID, source.ProcessID, source.ControllNumber, TRIM(source.EmployeeGroupName), source.ActionDate, TRIM(source.ActionUserID), source.ClaimKey, TRIM(source.InvoiceNumber), TRIM(source.Coid), source.RegionKey, source.GuarantorID, TRIM(source.AccountNumber), source.PRPNumber, TRIM(source.PPIKey), source.DocumentHandle, TRIM(source.BatchId), TRIM(source.SourceSystem), TRIM(source.Pool), TRIM(source.PoolFunction), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT LoadKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactUserProductivity
      GROUP BY LoadKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactUserProductivity');
ELSE
  COMMIT TRANSACTION;
END IF;
