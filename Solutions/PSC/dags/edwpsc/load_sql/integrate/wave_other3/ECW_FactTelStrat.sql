
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactTelStrat AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactTelStrat AS source
ON target.TelStratKey = source.TelStratKey
WHEN MATCHED THEN
  UPDATE SET
  target.TelStratKey = source.TelStratKey,
 target.TIME = source.TIME,
 target.EndTime = source.EndTime,
 target.UserAgent = TRIM(source.UserAgent),
 target.ANI = TRIM(source.ANI),
 target.DNIS = TRIM(source.DNIS),
 target.PORT = TRIM(source.PORT),
 target.PortFirstName = TRIM(source.PortFirstName),
 target.PortLastName = TRIM(source.PortLastName),
 target.Direction = TRIM(source.Direction),
 target.TotalHoldTime = source.TotalHoldTime,
 target.CallDuration = source.CallDuration,
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientID = TRIM(source.PatientID),
 target.PatientKey = source.PatientKey,
 target.PatientDOB = TRIM(source.PatientDOB),
 target.CustomerServiceRepID = TRIM(source.CustomerServiceRepID),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (TelStratKey, TIME, EndTime, UserAgent, ANI, DNIS, PORT, PortFirstName, PortLastName, Direction, TotalHoldTime, CallDuration, PatientFirstName, PatientLastName, PatientID, PatientKey, PatientDOB, CustomerServiceRepID, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.TelStratKey, source.TIME, source.EndTime, TRIM(source.UserAgent), TRIM(source.ANI), TRIM(source.DNIS), TRIM(source.PORT), TRIM(source.PortFirstName), TRIM(source.PortLastName), TRIM(source.Direction), source.TotalHoldTime, source.CallDuration, TRIM(source.PatientFirstName), TRIM(source.PatientLastName), TRIM(source.PatientID), source.PatientKey, TRIM(source.PatientDOB), TRIM(source.CustomerServiceRepID), TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TelStratKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactTelStrat
      GROUP BY TelStratKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactTelStrat');
ELSE
  COMMIT TRANSACTION;
END IF;
