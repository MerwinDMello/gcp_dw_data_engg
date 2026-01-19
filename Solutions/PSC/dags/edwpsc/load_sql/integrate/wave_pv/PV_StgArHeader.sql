
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgArHeader AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgArHeader AS source
ON target.ArHeaderPK = source.ArHeaderPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Inv_Num = source.Inv_Num,
 target.Proc_Code = TRIM(source.Proc_Code),
 target.Modifier = TRIM(source.Modifier),
 target.AR_Ctrl_Num = source.AR_Ctrl_Num,
 target.Log_Num = source.Log_Num,
 target.Clinic = TRIM(source.Clinic),
 target.TYPE = TRIM(source.TYPE),
 target.Pat_Num = source.Pat_Num,
 target.Pat_Name = TRIM(source.Pat_Name),
 target.Phy_Num = source.Phy_Num,
 target.Phy_Name = TRIM(source.Phy_Name),
 target.Payer_Type = TRIM(source.Payer_Type),
 target.Payer_Class = TRIM(source.Payer_Class),
 target.Payer_Num = source.Payer_Num,
 target.Payer_Name = TRIM(source.Payer_Name),
 target.Proc_Desc = TRIM(source.Proc_Desc),
 target.CPT_Code = TRIM(source.CPT_Code),
 target.POS = TRIM(source.POS),
 target.Svc_Date = source.Svc_Date,
 target.Inv_Date = source.Inv_Date,
 target.Closing_Date = source.Closing_Date,
 target.Crg_Amt = source.Crg_Amt,
 target.AR_Amt = source.AR_Amt,
 target.Adj_Amt = source.Adj_Amt,
 target.Paid_Amt = source.Paid_Amt,
 target.Crg_Balance = source.Crg_Balance,
 target.End_Balance = source.End_Balance,
 target.DB_Acnt = TRIM(source.DB_Acnt),
 target.CR_Acnt = TRIM(source.CR_Acnt),
 target.RVU = source.RVU,
 target.Rebill_Flag = TRIM(source.Rebill_Flag),
 target.Old_Rebill = TRIM(source.Old_Rebill),
 target.Rev_Adj_Flag = TRIM(source.Rev_Adj_Flag),
 target.Rev_Adj_Date = source.Rev_Adj_Date,
 target.Crossover_Code = TRIM(source.Crossover_Code),
 target.Crossover_Name = TRIM(source.Crossover_Name),
 target.Release_Flag = TRIM(source.Release_Flag),
 target.Action_Date = source.Action_Date,
 target.Work_List = TRIM(source.Work_List),
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_DateTime = source.Crt_DateTime,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.Payer_Subclass = source.Payer_Subclass,
 target.LastUpdateServerTime = source.LastUpdateServerTime,
 target.CrgDetailPK = TRIM(source.CrgDetailPK),
 target.CrgDetailPK_txt = TRIM(source.CrgDetailPK_txt),
 target.Note = TRIM(source.Note),
 target.Rebill_ICDType = source.Rebill_ICDType,
 target.ArHeaderPK = TRIM(source.ArHeaderPK),
 target.ArHeaderPK_txt = TRIM(source.ArHeaderPK_txt),
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
  INSERT (Practice, Inv_Num, Proc_Code, Modifier, AR_Ctrl_Num, Log_Num, Clinic, TYPE, Pat_Num, Pat_Name, Phy_Num, Phy_Name, Payer_Type, Payer_Class, Payer_Num, Payer_Name, Proc_Desc, CPT_Code, POS, Svc_Date, Inv_Date, Closing_Date, Crg_Amt, AR_Amt, Adj_Amt, Paid_Amt, Crg_Balance, End_Balance, DB_Acnt, CR_Acnt, RVU, Rebill_Flag, Old_Rebill, Rev_Adj_Flag, Rev_Adj_Date, Crossover_Code, Crossover_Name, Release_Flag, Action_Date, Work_List, Crt_UserID, Crt_DateTime, Last_Upd_UserID, Last_Upd_DateTime, Payer_Subclass, LastUpdateServerTime, CrgDetailPK, CrgDetailPK_txt, Note, Rebill_ICDType, ArHeaderPK, ArHeaderPK_txt, RegionKey, sysstarttime, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, SourceSystemCode, InsertedBy, ModifiedBy)
  VALUES (TRIM(source.Practice), source.Inv_Num, TRIM(source.Proc_Code), TRIM(source.Modifier), source.AR_Ctrl_Num, source.Log_Num, TRIM(source.Clinic), TRIM(source.TYPE), source.Pat_Num, TRIM(source.Pat_Name), source.Phy_Num, TRIM(source.Phy_Name), TRIM(source.Payer_Type), TRIM(source.Payer_Class), source.Payer_Num, TRIM(source.Payer_Name), TRIM(source.Proc_Desc), TRIM(source.CPT_Code), TRIM(source.POS), source.Svc_Date, source.Inv_Date, source.Closing_Date, source.Crg_Amt, source.AR_Amt, source.Adj_Amt, source.Paid_Amt, source.Crg_Balance, source.End_Balance, TRIM(source.DB_Acnt), TRIM(source.CR_Acnt), source.RVU, TRIM(source.Rebill_Flag), TRIM(source.Old_Rebill), TRIM(source.Rev_Adj_Flag), source.Rev_Adj_Date, TRIM(source.Crossover_Code), TRIM(source.Crossover_Name), TRIM(source.Release_Flag), source.Action_Date, TRIM(source.Work_List), TRIM(source.Crt_UserID), source.Crt_DateTime, TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, source.Payer_Subclass, source.LastUpdateServerTime, TRIM(source.CrgDetailPK), TRIM(source.CrgDetailPK_txt), TRIM(source.Note), source.Rebill_ICDType, TRIM(source.ArHeaderPK), TRIM(source.ArHeaderPK_txt), source.RegionKey, source.sysstarttime, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), TRIM(source.ModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ArHeaderPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgArHeader
      GROUP BY ArHeaderPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgArHeader');
ELSE
  COMMIT TRANSACTION;
END IF;
