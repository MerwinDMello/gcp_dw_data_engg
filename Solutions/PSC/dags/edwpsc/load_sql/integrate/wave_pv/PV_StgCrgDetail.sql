
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgCrgDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgCrgDetail AS source
ON target.CrgDetailPK = source.CrgDetailPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Inv_Num = source.Inv_Num,
 target.Line_Num = source.Line_Num,
 target.Log_Num = source.Log_Num,
 target.Status = TRIM(source.Status),
 target.Pat_Num = source.Pat_Num,
 target.Svc_Date = source.Svc_Date,
 target.Payer_Class = TRIM(source.Payer_Class),
 target.Payer_Type = TRIM(source.Payer_Type),
 target.Payer_Num = source.Payer_Num,
 target.Payer_Name = TRIM(source.Payer_Name),
 target.Phy_Num = source.Phy_Num,
 target.Phy_Name = TRIM(source.Phy_Name),
 target.Group_ID = TRIM(source.Group_ID),
 target.Provider_ID = TRIM(source.Provider_ID),
 target.Fed_Tax_ID = TRIM(source.Fed_Tax_ID),
 target.BillTo_LastName = TRIM(source.BillTo_LastName),
 target.BillTo_FirstName = TRIM(source.BillTo_FirstName),
 target.BillTo_Credential = TRIM(source.BillTo_Credential),
 target.Proc_Code = TRIM(source.Proc_Code),
 target.Modifier = TRIM(source.Modifier),
 target.CPT_Code = TRIM(source.CPT_Code),
 target.Proc_Desc = TRIM(source.Proc_Desc),
 target.Svc_Type = TRIM(source.Svc_Type),
 target.TYPE = TRIM(source.TYPE),
 target.Rev_Code = TRIM(source.Rev_Code),
 target.RVU = source.RVU,
 target.Diag1 = TRIM(source.Diag1),
 target.Diag2 = TRIM(source.Diag2),
 target.Diag3 = TRIM(source.Diag3),
 target.Diag4 = TRIM(source.Diag4),
 target.Quantity = source.Quantity,
 target.Dosage = source.Dosage,
 target.Crg_Amt = source.Crg_Amt,
 target.Override_Amt = source.Override_Amt,
 target.AR_Amt = source.AR_Amt,
 target.Crg_Paid_Amt = source.Crg_Paid_Amt,
 target.Pat_Paid_Amt = source.Pat_Paid_Amt,
 target.Payer_Paid_Amt = source.Payer_Paid_Amt,
 target.Adj_Code = TRIM(source.Adj_Code),
 target.Adj_Amt = source.Adj_Amt,
 target.Crg_Adj_Amt = source.Crg_Adj_Amt,
 target.Crg_Balance = source.Crg_Balance,
 target.Payer_Flag = TRIM(source.Payer_Flag),
 target.Deleted = TRIM(source.Deleted),
 target.Rebill_Flag = TRIM(source.Rebill_Flag),
 target.Status_Date = source.Status_Date,
 target.Sent_Date = source.Sent_Date,
 target.Closing_Date = source.Closing_Date,
 target.Last_Upd_UserNum = source.Last_Upd_UserNum,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.CrgDetailPK = TRIM(source.CrgDetailPK),
 target.CrgDetailPK_txt = TRIM(source.CrgDetailPK_txt),
 target.UB_Rev_Code = TRIM(source.UB_Rev_Code),
 target.DescriptionSentInEDI = source.DescriptionSentInEDI,
 target.CrgHeaderPK = TRIM(source.CrgHeaderPK),
 target.CrgHeaderPK_txt = TRIM(source.CrgHeaderPK_txt),
 target.RegionKey = source.RegionKey,
 target.sysstarttime = source.sysstarttime,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.ModifiedBy = TRIM(source.ModifiedBy)
WHEN NOT MATCHED THEN
  INSERT (Practice, Inv_Num, Line_Num, Log_Num, Status, Pat_Num, Svc_Date, Payer_Class, Payer_Type, Payer_Num, Payer_Name, Phy_Num, Phy_Name, Group_ID, Provider_ID, Fed_Tax_ID, BillTo_LastName, BillTo_FirstName, BillTo_Credential, Proc_Code, Modifier, CPT_Code, Proc_Desc, Svc_Type, TYPE, Rev_Code, RVU, Diag1, Diag2, Diag3, Diag4, Quantity, Dosage, Crg_Amt, Override_Amt, AR_Amt, Crg_Paid_Amt, Pat_Paid_Amt, Payer_Paid_Amt, Adj_Code, Adj_Amt, Crg_Adj_Amt, Crg_Balance, Payer_Flag, Deleted, Rebill_Flag, Status_Date, Sent_Date, Closing_Date, Last_Upd_UserNum, Last_Upd_UserID, Last_Upd_DateTime, CrgDetailPK, CrgDetailPK_txt, UB_Rev_Code, DescriptionSentInEDI, CrgHeaderPK, CrgHeaderPK_txt, RegionKey, sysstarttime, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, SourceSystemCode, InsertedBy, ModifiedBy)
  VALUES (TRIM(source.Practice), source.Inv_Num, source.Line_Num, source.Log_Num, TRIM(source.Status), source.Pat_Num, source.Svc_Date, TRIM(source.Payer_Class), TRIM(source.Payer_Type), source.Payer_Num, TRIM(source.Payer_Name), source.Phy_Num, TRIM(source.Phy_Name), TRIM(source.Group_ID), TRIM(source.Provider_ID), TRIM(source.Fed_Tax_ID), TRIM(source.BillTo_LastName), TRIM(source.BillTo_FirstName), TRIM(source.BillTo_Credential), TRIM(source.Proc_Code), TRIM(source.Modifier), TRIM(source.CPT_Code), TRIM(source.Proc_Desc), TRIM(source.Svc_Type), TRIM(source.TYPE), TRIM(source.Rev_Code), source.RVU, TRIM(source.Diag1), TRIM(source.Diag2), TRIM(source.Diag3), TRIM(source.Diag4), source.Quantity, source.Dosage, source.Crg_Amt, source.Override_Amt, source.AR_Amt, source.Crg_Paid_Amt, source.Pat_Paid_Amt, source.Payer_Paid_Amt, TRIM(source.Adj_Code), source.Adj_Amt, source.Crg_Adj_Amt, source.Crg_Balance, TRIM(source.Payer_Flag), TRIM(source.Deleted), TRIM(source.Rebill_Flag), source.Status_Date, source.Sent_Date, source.Closing_Date, source.Last_Upd_UserNum, TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, TRIM(source.CrgDetailPK), TRIM(source.CrgDetailPK_txt), TRIM(source.UB_Rev_Code), source.DescriptionSentInEDI, TRIM(source.CrgHeaderPK), TRIM(source.CrgHeaderPK_txt), source.RegionKey, source.sysstarttime, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), TRIM(source.ModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CrgDetailPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgCrgDetail
      GROUP BY CrgDetailPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgCrgDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
