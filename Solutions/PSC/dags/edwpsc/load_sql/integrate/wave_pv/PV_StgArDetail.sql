
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgArDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgArDetail AS source
ON target.ArDetailPK = source.ArDetailPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Inv_Num = source.Inv_Num,
 target.Proc_Code = TRIM(source.Proc_Code),
 target.Modifier = TRIM(source.Modifier),
 target.AR_Ctrl_Num = source.AR_Ctrl_Num,
 target.Trans_Num = source.Trans_Num,
 target.Log_Num = source.Log_Num,
 target.Pat_Num = source.Pat_Num,
 target.TYPE = TRIM(source.TYPE),
 target.Payer_Type = TRIM(source.Payer_Type),
 target.Payer_Class = TRIM(source.Payer_Class),
 target.Payer_Num = source.Payer_Num,
 target.Payer_Name = TRIM(source.Payer_Name),
 target.Trans_Date = source.Trans_Date,
 target.Trans_Type = TRIM(source.Trans_Type),
 target.Trans_Amt = source.Trans_Amt,
 target.Trans_Desc = TRIM(source.Trans_Desc),
 target.DB_Acnt = TRIM(source.DB_Acnt),
 target.CR_Acnt = TRIM(source.CR_Acnt),
 target.Pymt_Type = TRIM(source.Pymt_Type),
 target.Check_Num = TRIM(source.Check_Num),
 target.Payment_Num = source.Payment_Num,
 target.Reason = TRIM(source.Reason),
 target.Reverse_Flag = TRIM(source.Reverse_Flag),
 target.Delete_Flag = TRIM(source.Delete_Flag),
 target.Display_Flag = TRIM(source.Display_Flag),
 target.Closing_Date = source.Closing_Date,
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_DateTime = source.Crt_DateTime,
 target.Payer_Subclass = source.Payer_Subclass,
 target.LastUpdateServerTime = source.LastUpdateServerTime,
 target.ArDetailPK = TRIM(source.ArDetailPK),
 target.ArDetailPK_txt = TRIM(source.ArDetailPK_txt),
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
  INSERT (Practice, Inv_Num, Proc_Code, Modifier, AR_Ctrl_Num, Trans_Num, Log_Num, Pat_Num, TYPE, Payer_Type, Payer_Class, Payer_Num, Payer_Name, Trans_Date, Trans_Type, Trans_Amt, Trans_Desc, DB_Acnt, CR_Acnt, Pymt_Type, Check_Num, Payment_Num, Reason, Reverse_Flag, Delete_Flag, Display_Flag, Closing_Date, Crt_UserID, Crt_DateTime, Payer_Subclass, LastUpdateServerTime, ArDetailPK, ArDetailPK_txt, RegionKey, sysstarttime, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, SourceSystemCode, InsertedBy, ModifiedBy)
  VALUES (TRIM(source.Practice), source.Inv_Num, TRIM(source.Proc_Code), TRIM(source.Modifier), source.AR_Ctrl_Num, source.Trans_Num, source.Log_Num, source.Pat_Num, TRIM(source.TYPE), TRIM(source.Payer_Type), TRIM(source.Payer_Class), source.Payer_Num, TRIM(source.Payer_Name), source.Trans_Date, TRIM(source.Trans_Type), source.Trans_Amt, TRIM(source.Trans_Desc), TRIM(source.DB_Acnt), TRIM(source.CR_Acnt), TRIM(source.Pymt_Type), TRIM(source.Check_Num), source.Payment_Num, TRIM(source.Reason), TRIM(source.Reverse_Flag), TRIM(source.Delete_Flag), TRIM(source.Display_Flag), source.Closing_Date, TRIM(source.Crt_UserID), source.Crt_DateTime, source.Payer_Subclass, source.LastUpdateServerTime, TRIM(source.ArDetailPK), TRIM(source.ArDetailPK_txt), source.RegionKey, source.sysstarttime, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), TRIM(source.ModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ArDetailPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgArDetail
      GROUP BY ArDetailPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgArDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
