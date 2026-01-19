
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgLogBook AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgLogBook AS source
ON target.LogBookPK = source.LogBookPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Log_Num = source.Log_Num,
 target.Practice = TRIM(source.Practice),
 target.Pat_Num = source.Pat_Num,
 target.Svc_Date = source.Svc_Date,
 target.Clinic = TRIM(source.Clinic),
 target.Last_Name = TRIM(source.Last_Name),
 target.First_Name = TRIM(source.First_Name),
 target.Birthday = source.Birthday,
 target.SSN = TRIM(source.SSN),
 target.Phone = TRIM(source.Phone),
 target.Phone_Ext = TRIM(source.Phone_Ext),
 target.PRV_Status = TRIM(source.PRV_Status),
 target.EPS_Status = TRIM(source.EPS_Status),
 target.WC_Status = TRIM(source.WC_Status),
 target.Misc_Status = TRIM(source.Misc_Status),
 target.Time_In = source.Time_In,
 target.scheduledTime = source.scheduledTime,
 target.scheduledMethod = source.scheduledMethod,
 target.Time_Out = source.Time_Out,
 target.Hear_From = TRIM(source.Hear_From),
 target.Crt_UserNum = source.Crt_UserNum,
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_DateTime = source.Crt_DateTime,
 target.Last_Upd_UserNum = source.Last_Upd_UserNum,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.ZipPass_CheckIn = source.ZipPass_CheckIn,
 target.LogBookPK = TRIM(source.LogBookPK),
 target.Notes = TRIM(source.Notes),
 target.PatAppointmentPK = TRIM(source.PatAppointmentPK),
 target.KioskSessionPK = TRIM(source.KioskSessionPK),
 target.KioskPIN = TRIM(source.KioskPIN),
 target.KioskStatus = source.KioskStatus,
 target.RegionKey = source.RegionKey,
 target.VisitStatusId = source.VisitStatusId,
 target.eRegStatus = source.eRegStatus,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Log_Num, Practice, Pat_Num, Svc_Date, Clinic, Last_Name, First_Name, Birthday, SSN, Phone, Phone_Ext, PRV_Status, EPS_Status, WC_Status, Misc_Status, Time_In, scheduledTime, scheduledMethod, Time_Out, Hear_From, Crt_UserNum, Crt_UserID, Crt_DateTime, Last_Upd_UserNum, Last_Upd_UserID, Last_Upd_DateTime, ZipPass_CheckIn, LogBookPK, Notes, PatAppointmentPK, KioskSessionPK, KioskPIN, KioskStatus, RegionKey, VisitStatusId, eRegStatus, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (source.Log_Num, TRIM(source.Practice), source.Pat_Num, source.Svc_Date, TRIM(source.Clinic), TRIM(source.Last_Name), TRIM(source.First_Name), source.Birthday, TRIM(source.SSN), TRIM(source.Phone), TRIM(source.Phone_Ext), TRIM(source.PRV_Status), TRIM(source.EPS_Status), TRIM(source.WC_Status), TRIM(source.Misc_Status), source.Time_In, source.scheduledTime, source.scheduledMethod, source.Time_Out, TRIM(source.Hear_From), source.Crt_UserNum, TRIM(source.Crt_UserID), source.Crt_DateTime, source.Last_Upd_UserNum, TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, source.ZipPass_CheckIn, TRIM(source.LogBookPK), TRIM(source.Notes), TRIM(source.PatAppointmentPK), TRIM(source.KioskSessionPK), TRIM(source.KioskPIN), source.KioskStatus, source.RegionKey, source.VisitStatusId, source.eRegStatus, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT LogBookPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgLogBook
      GROUP BY LogBookPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgLogBook');
ELSE
  COMMIT TRANSACTION;
END IF;
