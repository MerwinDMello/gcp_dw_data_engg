
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgCrgHeader AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgCrgHeader AS source
ON target.CrgHeaderPK = source.CrgHeaderPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Inv_Num = source.Inv_Num,
 target.Log_Num = source.Log_Num,
 target.TYPE = TRIM(source.TYPE),
 target.Status = TRIM(source.Status),
 target.Pat_Num = source.Pat_Num,
 target.Pat_Name = TRIM(source.Pat_Name),
 target.POS = TRIM(source.POS),
 target.Clinic = TRIM(source.Clinic),
 target.Facility = TRIM(source.Facility),
 target.Svc_Date = source.Svc_Date,
 target.Phy_Num = source.Phy_Num,
 target.Phy_Name = TRIM(source.Phy_Name),
 target.Ref_Phy_Num = source.Ref_Phy_Num,
 target.Ref_Phy_Name = TRIM(source.Ref_Phy_Name),
 target.Provider_ID = TRIM(source.Provider_ID),
 target.Group_ID = TRIM(source.Group_ID),
 target.Fed_Tax_ID = TRIM(source.Fed_Tax_ID),
 target.BillTo_LastName = TRIM(source.BillTo_LastName),
 target.BillTo_FirstName = TRIM(source.BillTo_FirstName),
 target.BillTo_Credential = TRIM(source.BillTo_Credential),
 target.Auth_Num = TRIM(source.Auth_Num),
 target.Withhold_Code = TRIM(source.Withhold_Code),
 target.EMC_PID = TRIM(source.EMC_PID),
 target.Payer_Class = TRIM(source.Payer_Class),
 target.Payer_Type = TRIM(source.Payer_Type),
 target.Payer_Num = source.Payer_Num,
 target.Payer_Name = TRIM(source.Payer_Name),
 target.Pricing = TRIM(source.Pricing),
 target.Protocol = TRIM(source.Protocol),
 target.Diag1 = TRIM(source.Diag1),
 target.Diag2 = TRIM(source.Diag2),
 target.Diag3 = TRIM(source.Diag3),
 target.Diag4 = TRIM(source.Diag4),
 target.Box19 = TRIM(source.Box19),
 target.Outside_Lab = TRIM(source.Outside_Lab),
 target.Emp_Related = TRIM(source.Emp_Related),
 target.Auto_Accident = TRIM(source.Auto_Accident),
 target.Auto_Accident_State = TRIM(source.Auto_Accident_State),
 target.Accident_Flag = TRIM(source.Accident_Flag),
 target.First_DOI = source.First_DOI,
 target.Current_DOI = source.Current_DOI,
 target.Total_Charge = source.Total_Charge,
 target.Total_Override = source.Total_Override,
 target.Total_AR = source.Total_AR,
 target.Total_Paid = source.Total_Paid,
 target.Total_Adj = source.Total_Adj,
 target.Crg_Balance = source.Crg_Balance,
 target.Copay_Req = source.Copay_Req,
 target.Copay_Paid = source.Copay_Paid,
 target.Copay_Type = TRIM(source.Copay_Type),
 target.Copay_Notes = TRIM(source.Copay_Notes),
 target.Curr_Pay_Amt = source.Curr_Pay_Amt,
 target.Curr_Pay_Type = TRIM(source.Curr_Pay_Type),
 target.Curr_Pay_Notes = TRIM(source.Curr_Pay_Notes),
 target.Payment_Num = source.Payment_Num,
 target.Prev_Payment_Num = source.Prev_Payment_Num,
 target.Prev_BillTo_Payer = source.Prev_BillTo_Payer,
 target.Prev_Pay_Amt = source.Prev_Pay_Amt,
 target.Prev_Pay_Type = TRIM(source.Prev_Pay_Type),
 target.Prev_Pay_Notes = TRIM(source.Prev_Pay_Notes),
 target.First_Bill_Date = source.First_Bill_Date,
 target.Fiscal_Date = source.Fiscal_Date,
 target.EDI_Flag = TRIM(source.EDI_Flag),
 target.Rebill_Flag = TRIM(source.Rebill_Flag),
 target.Audit_Trail = TRIM(source.Audit_Trail),
 target.Category = TRIM(source.Category),
 target.Ref_Clinic = TRIM(source.Ref_Clinic),
 target.Sent_Date = source.Sent_Date,
 target.Print_Date = source.Print_Date,
 target.Closing_Date = source.Closing_Date,
 target.Prev_Status = TRIM(source.Prev_Status),
 target.Prev_Sent_Date = source.Prev_Sent_Date,
 target.Prev_First_Bill_Date = source.Prev_First_Bill_Date,
 target.New_Inv_Num = source.New_Inv_Num,
 target.Old_Inv_Num = source.Old_Inv_Num,
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_UserNum = source.Crt_UserNum,
 target.Crt_DateTime = source.Crt_DateTime,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_UserNum = source.Last_Upd_UserNum,
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.Sent_Date_2nd = source.Sent_Date_2nd,
 target.Payer_Type_2nd = TRIM(source.Payer_Type_2nd),
 target.Payer_Num_2nd = source.Payer_Num_2nd,
 target.Payer_Name_2nd = TRIM(source.Payer_Name_2nd),
 target.EMC_PID_2nd = TRIM(source.EMC_PID_2nd),
 target.Group_ID_2nd = TRIM(source.Group_ID_2nd),
 target.Provider_ID_2nd = TRIM(source.Provider_ID_2nd),
 target.NDC_Flag = TRIM(source.NDC_Flag),
 target.CrgHeaderPK = TRIM(source.CrgHeaderPK),
 target.SelfPay_Flag = TRIM(source.SelfPay_Flag),
 target.Payer_Subclass = source.Payer_Subclass,
 target.EDI_Type = TRIM(source.EDI_Type),
 target.LogDetail_Cache = TRIM(source.LogDetail_Cache),
 target.LogDetailPK = TRIM(source.LogDetailPK),
 target.Insurance_Type = TRIM(source.Insurance_Type),
 target.SuppressZeroCharges = source.SuppressZeroCharges,
 target.Payer_Resent = source.Payer_Resent,
 target.CCPlanChargeDate = source.CCPlanChargeDate,
 target.CCPlanNotifyType = source.CCPlanNotifyType,
 target.HasAttachment = source.HasAttachment,
 target.EpsdtReferral = source.EpsdtReferral,
 target.Box17RefPhyQualifier = TRIM(source.Box17RefPhyQualifier),
 target.PayerClaimNum = TRIM(source.PayerClaimNum),
 target.AdmitDate = source.AdmitDate,
 target.DischargeDate = source.DischargeDate,
 target.AttachmentType = TRIM(source.AttachmentType),
 target.BilledICDType = source.BilledICDType,
 target.SecondaryBilledICDType = source.SecondaryBilledICDType,
 target.WcAttachmentHeaderID = TRIM(source.WcAttachmentHeaderID),
 target.ClonedFromCrgHeaderPK = TRIM(source.ClonedFromCrgHeaderPK),
 target.RegionKey = source.RegionKey,
 target.StatementNum = source.StatementNum,
 target.StatementDate = source.StatementDate,
 target.AdmitTime = source.AdmitTime,
 target.AdmitType = source.AdmitType,
 target.AdmitSource = TRIM(source.AdmitSource),
 target.DischargeTime = source.DischargeTime,
 target.DischargeStatus = TRIM(source.DischargeStatus),
 target.sysstarttime = source.sysstarttime,
 target.sysendtime = source.sysendtime,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
 target.CrgHeaderPK_txt = TRIM(source.CrgHeaderPK_txt),
 target.LogDetailPK_txt = TRIM(source.LogDetailPK_txt)
WHEN NOT MATCHED THEN
  INSERT (Practice, Inv_Num, Log_Num, TYPE, Status, Pat_Num, Pat_Name, POS, Clinic, Facility, Svc_Date, Phy_Num, Phy_Name, Ref_Phy_Num, Ref_Phy_Name, Provider_ID, Group_ID, Fed_Tax_ID, BillTo_LastName, BillTo_FirstName, BillTo_Credential, Auth_Num, Withhold_Code, EMC_PID, Payer_Class, Payer_Type, Payer_Num, Payer_Name, Pricing, Protocol, Diag1, Diag2, Diag3, Diag4, Box19, Outside_Lab, Emp_Related, Auto_Accident, Auto_Accident_State, Accident_Flag, First_DOI, Current_DOI, Total_Charge, Total_Override, Total_AR, Total_Paid, Total_Adj, Crg_Balance, Copay_Req, Copay_Paid, Copay_Type, Copay_Notes, Curr_Pay_Amt, Curr_Pay_Type, Curr_Pay_Notes, Payment_Num, Prev_Payment_Num, Prev_BillTo_Payer, Prev_Pay_Amt, Prev_Pay_Type, Prev_Pay_Notes, First_Bill_Date, Fiscal_Date, EDI_Flag, Rebill_Flag, Audit_Trail, Category, Ref_Clinic, Sent_Date, Print_Date, Closing_Date, Prev_Status, Prev_Sent_Date, Prev_First_Bill_Date, New_Inv_Num, Old_Inv_Num, Crt_UserID, Crt_UserNum, Crt_DateTime, Last_Upd_UserID, Last_Upd_UserNum, Last_Upd_DateTime, Sent_Date_2nd, Payer_Type_2nd, Payer_Num_2nd, Payer_Name_2nd, EMC_PID_2nd, Group_ID_2nd, Provider_ID_2nd, NDC_Flag, CrgHeaderPK, SelfPay_Flag, Payer_Subclass, EDI_Type, LogDetail_Cache, LogDetailPK, Insurance_Type, SuppressZeroCharges, Payer_Resent, CCPlanChargeDate, CCPlanNotifyType, HasAttachment, EpsdtReferral, Box17RefPhyQualifier, PayerClaimNum, AdmitDate, DischargeDate, AttachmentType, BilledICDType, SecondaryBilledICDType, WcAttachmentHeaderID, ClonedFromCrgHeaderPK, RegionKey, StatementNum, StatementDate, AdmitTime, AdmitType, AdmitSource, DischargeTime, DischargeStatus, sysstarttime, sysendtime, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, CrgHeaderPK_txt, LogDetailPK_txt)
  VALUES (TRIM(source.Practice), source.Inv_Num, source.Log_Num, TRIM(source.TYPE), TRIM(source.Status), source.Pat_Num, TRIM(source.Pat_Name), TRIM(source.POS), TRIM(source.Clinic), TRIM(source.Facility), source.Svc_Date, source.Phy_Num, TRIM(source.Phy_Name), source.Ref_Phy_Num, TRIM(source.Ref_Phy_Name), TRIM(source.Provider_ID), TRIM(source.Group_ID), TRIM(source.Fed_Tax_ID), TRIM(source.BillTo_LastName), TRIM(source.BillTo_FirstName), TRIM(source.BillTo_Credential), TRIM(source.Auth_Num), TRIM(source.Withhold_Code), TRIM(source.EMC_PID), TRIM(source.Payer_Class), TRIM(source.Payer_Type), source.Payer_Num, TRIM(source.Payer_Name), TRIM(source.Pricing), TRIM(source.Protocol), TRIM(source.Diag1), TRIM(source.Diag2), TRIM(source.Diag3), TRIM(source.Diag4), TRIM(source.Box19), TRIM(source.Outside_Lab), TRIM(source.Emp_Related), TRIM(source.Auto_Accident), TRIM(source.Auto_Accident_State), TRIM(source.Accident_Flag), source.First_DOI, source.Current_DOI, source.Total_Charge, source.Total_Override, source.Total_AR, source.Total_Paid, source.Total_Adj, source.Crg_Balance, source.Copay_Req, source.Copay_Paid, TRIM(source.Copay_Type), TRIM(source.Copay_Notes), source.Curr_Pay_Amt, TRIM(source.Curr_Pay_Type), TRIM(source.Curr_Pay_Notes), source.Payment_Num, source.Prev_Payment_Num, source.Prev_BillTo_Payer, source.Prev_Pay_Amt, TRIM(source.Prev_Pay_Type), TRIM(source.Prev_Pay_Notes), source.First_Bill_Date, source.Fiscal_Date, TRIM(source.EDI_Flag), TRIM(source.Rebill_Flag), TRIM(source.Audit_Trail), TRIM(source.Category), TRIM(source.Ref_Clinic), source.Sent_Date, source.Print_Date, source.Closing_Date, TRIM(source.Prev_Status), source.Prev_Sent_Date, source.Prev_First_Bill_Date, source.New_Inv_Num, source.Old_Inv_Num, TRIM(source.Crt_UserID), source.Crt_UserNum, source.Crt_DateTime, TRIM(source.Last_Upd_UserID), source.Last_Upd_UserNum, source.Last_Upd_DateTime, source.Sent_Date_2nd, TRIM(source.Payer_Type_2nd), source.Payer_Num_2nd, TRIM(source.Payer_Name_2nd), TRIM(source.EMC_PID_2nd), TRIM(source.Group_ID_2nd), TRIM(source.Provider_ID_2nd), TRIM(source.NDC_Flag), TRIM(source.CrgHeaderPK), TRIM(source.SelfPay_Flag), source.Payer_Subclass, TRIM(source.EDI_Type), TRIM(source.LogDetail_Cache), TRIM(source.LogDetailPK), TRIM(source.Insurance_Type), source.SuppressZeroCharges, source.Payer_Resent, source.CCPlanChargeDate, source.CCPlanNotifyType, source.HasAttachment, source.EpsdtReferral, TRIM(source.Box17RefPhyQualifier), TRIM(source.PayerClaimNum), source.AdmitDate, source.DischargeDate, TRIM(source.AttachmentType), source.BilledICDType, source.SecondaryBilledICDType, TRIM(source.WcAttachmentHeaderID), TRIM(source.ClonedFromCrgHeaderPK), source.RegionKey, source.StatementNum, source.StatementDate, source.AdmitTime, source.AdmitType, TRIM(source.AdmitSource), source.DischargeTime, TRIM(source.DischargeStatus), source.sysstarttime, source.sysendtime, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.CrgHeaderPK_txt), TRIM(source.LogDetailPK_txt));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CrgHeaderPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgCrgHeader
      GROUP BY CrgHeaderPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgCrgHeader');
ELSE
  COMMIT TRANSACTION;
END IF;
