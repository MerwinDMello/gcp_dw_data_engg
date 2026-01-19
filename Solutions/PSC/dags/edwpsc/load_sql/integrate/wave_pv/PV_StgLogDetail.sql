
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgLogDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgLogDetail AS source
ON target.LogDetailPK = source.LogDetailPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Log_Num = source.Log_Num,
 target.Practice = TRIM(source.Practice),
 target.Clinic = TRIM(source.Clinic),
 target.Pat_Num = source.Pat_Num,
 target.Svc_Date = source.Svc_Date,
 target.TYPE = TRIM(source.TYPE),
 target.Cmp_Num = source.Cmp_Num,
 target.Cmp_Name = TRIM(source.Cmp_Name),
 target.Protocol = TRIM(source.Protocol),
 target.Phy_Num = source.Phy_Num,
 target.Phy_Name = TRIM(source.Phy_Name),
 target.EM_Code = TRIM(source.EM_Code),
 target.Diag_Codes = TRIM(source.Diag_Codes),
 target.Status = TRIM(source.Status),
 target.Notes = TRIM(source.Notes),
 target.Time_In = source.Time_In,
 target.Time_Out = source.Time_Out,
 target.New_WC_Flag = TRIM(source.New_WC_Flag),
 target.Copay_Amt = source.Copay_Amt,
 target.Copay_Type = TRIM(source.Copay_Type),
 target.Copay_Notes = TRIM(source.Copay_Notes),
 target.Prev_Pay_Amt = source.Prev_Pay_Amt,
 target.Prev_Pay_Type = TRIM(source.Prev_Pay_Type),
 target.Prev_Pay_Notes = TRIM(source.Prev_Pay_Notes),
 target.Curr_Pay_Amt = source.Curr_Pay_Amt,
 target.Curr_Pay_Type = TRIM(source.Curr_Pay_Type),
 target.Curr_Pay_Notes = TRIM(source.Curr_Pay_Notes),
 target.Crg_Num = source.Crg_Num,
 target.PV_Msg = TRIM(source.PV_Msg),
 target.PV_Date = source.PV_Date,
 target.PV_DateTime = source.PV_DateTime,
 target.PV_Status = TRIM(source.PV_Status),
 target.WC_Notes = TRIM(source.WC_Notes),
 target.No_Pivot_Flag = TRIM(source.No_Pivot_Flag),
 target.POA_Pymt_Num = source.POA_Pymt_Num,
 target.Category = TRIM(source.Category),
 target.Ref_Clinic = TRIM(source.Ref_Clinic),
 target.Crt_UserNum = source.Crt_UserNum,
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_DateTime = source.Crt_DateTime,
 target.Last_Upd_UserNum = source.Last_Upd_UserNum,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.Proc_Codes = TRIM(source.Proc_Codes),
 target.Quick_Visit_Type = TRIM(source.Quick_Visit_Type),
 target.Quick_Visit_Flag = TRIM(source.Quick_Visit_Flag),
 target.Ref_Phy_Num = source.Ref_Phy_Num,
 target.Visit_Locked = TRIM(source.Visit_Locked),
 target.Ins_Short_Name = TRIM(source.Ins_Short_Name),
 target.RTE_Status = source.RTE_Status,
 target.VisitChartType = source.VisitChartType,
 target.LogDetailID = source.LogDetailID,
 target.RoomNumber = TRIM(source.RoomNumber),
 target.LogDetailPK = TRIM(source.LogDetailPK),
 target.SelfPay_Flag = TRIM(source.SelfPay_Flag),
 target.ClinicPK = TRIM(source.ClinicPK),
 target.LastUpdateServerTime = source.LastUpdateServerTime,
 target.KioskPK = TRIM(source.KioskPK),
 target.ReferringPhysicianPK = TRIM(source.ReferringPhysicianPK),
 target.PatInfoPK = TRIM(source.PatInfoPK),
 target.VirtualVisitType = source.VirtualVisitType,
 target.AlternateLocation = TRIM(source.AlternateLocation),
 target.MedPodID = TRIM(source.MedPodID),
 target.IsChartSigned = source.IsChartSigned,
 target.PreferredPharmacy_NCPDP = TRIM(source.PreferredPharmacy_NCPDP),
 target.WantsInHouseDispensing = source.WantsInHouseDispensing,
 target.PreferredPharmacy_Confirmed = source.PreferredPharmacy_Confirmed,
 target.CompanyPK = TRIM(source.CompanyPK),
 target.AppointmentId = source.AppointmentId,
 target.IsEmployerPortalSent = source.IsEmployerPortalSent,
 target.RegionKey = source.RegionKey,
 target.ManagingPhyName = TRIM(source.ManagingPhyName),
 target.ManagingPhyNum = source.ManagingPhyNum,
 target.IsPatientTelemedicineEligible = source.IsPatientTelemedicineEligible,
 target.TelemedicineType = source.TelemedicineType,
 target.TelemedicineStatus = source.TelemedicineStatus,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Log_Num, Practice, Clinic, Pat_Num, Svc_Date, TYPE, Cmp_Num, Cmp_Name, Protocol, Phy_Num, Phy_Name, EM_Code, Diag_Codes, Status, Notes, Time_In, Time_Out, New_WC_Flag, Copay_Amt, Copay_Type, Copay_Notes, Prev_Pay_Amt, Prev_Pay_Type, Prev_Pay_Notes, Curr_Pay_Amt, Curr_Pay_Type, Curr_Pay_Notes, Crg_Num, PV_Msg, PV_Date, PV_DateTime, PV_Status, WC_Notes, No_Pivot_Flag, POA_Pymt_Num, Category, Ref_Clinic, Crt_UserNum, Crt_UserID, Crt_DateTime, Last_Upd_UserNum, Last_Upd_UserID, Last_Upd_DateTime, Proc_Codes, Quick_Visit_Type, Quick_Visit_Flag, Ref_Phy_Num, Visit_Locked, Ins_Short_Name, RTE_Status, VisitChartType, LogDetailID, RoomNumber, LogDetailPK, SelfPay_Flag, ClinicPK, LastUpdateServerTime, KioskPK, ReferringPhysicianPK, PatInfoPK, VirtualVisitType, AlternateLocation, MedPodID, IsChartSigned, PreferredPharmacy_NCPDP, WantsInHouseDispensing, PreferredPharmacy_Confirmed, CompanyPK, AppointmentId, IsEmployerPortalSent, RegionKey, ManagingPhyName, ManagingPhyNum, IsPatientTelemedicineEligible, TelemedicineType, TelemedicineStatus, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (source.Log_Num, TRIM(source.Practice), TRIM(source.Clinic), source.Pat_Num, source.Svc_Date, TRIM(source.TYPE), source.Cmp_Num, TRIM(source.Cmp_Name), TRIM(source.Protocol), source.Phy_Num, TRIM(source.Phy_Name), TRIM(source.EM_Code), TRIM(source.Diag_Codes), TRIM(source.Status), TRIM(source.Notes), source.Time_In, source.Time_Out, TRIM(source.New_WC_Flag), source.Copay_Amt, TRIM(source.Copay_Type), TRIM(source.Copay_Notes), source.Prev_Pay_Amt, TRIM(source.Prev_Pay_Type), TRIM(source.Prev_Pay_Notes), source.Curr_Pay_Amt, TRIM(source.Curr_Pay_Type), TRIM(source.Curr_Pay_Notes), source.Crg_Num, TRIM(source.PV_Msg), source.PV_Date, source.PV_DateTime, TRIM(source.PV_Status), TRIM(source.WC_Notes), TRIM(source.No_Pivot_Flag), source.POA_Pymt_Num, TRIM(source.Category), TRIM(source.Ref_Clinic), source.Crt_UserNum, TRIM(source.Crt_UserID), source.Crt_DateTime, source.Last_Upd_UserNum, TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, TRIM(source.Proc_Codes), TRIM(source.Quick_Visit_Type), TRIM(source.Quick_Visit_Flag), source.Ref_Phy_Num, TRIM(source.Visit_Locked), TRIM(source.Ins_Short_Name), source.RTE_Status, source.VisitChartType, source.LogDetailID, TRIM(source.RoomNumber), TRIM(source.LogDetailPK), TRIM(source.SelfPay_Flag), TRIM(source.ClinicPK), source.LastUpdateServerTime, TRIM(source.KioskPK), TRIM(source.ReferringPhysicianPK), TRIM(source.PatInfoPK), source.VirtualVisitType, TRIM(source.AlternateLocation), TRIM(source.MedPodID), source.IsChartSigned, TRIM(source.PreferredPharmacy_NCPDP), source.WantsInHouseDispensing, source.PreferredPharmacy_Confirmed, TRIM(source.CompanyPK), source.AppointmentId, source.IsEmployerPortalSent, source.RegionKey, TRIM(source.ManagingPhyName), source.ManagingPhyNum, source.IsPatientTelemedicineEligible, source.TelemedicineType, source.TelemedicineStatus, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT LogDetailPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgLogDetail
      GROUP BY LogDetailPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgLogDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
