
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgPayment AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgPayment AS source
ON target.PaymentPK = source.PaymentPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Payment_Num = source.Payment_Num,
 target.Batch_Num = source.Batch_Num,
 target.Seq_Num = TRIM(source.Seq_Num),
 target.Category = TRIM(source.Category),
 target.Practice = TRIM(source.Practice),
 target.Clinic = TRIM(source.Clinic),
 target.Check_Num = TRIM(source.Check_Num),
 target.Check_Date = source.Check_Date,
 target.Check_Amt = source.Check_Amt,
 target.Remain_Amt = source.Remain_Amt,
 target.Deposit_Date = source.Deposit_Date,
 target.Description = TRIM(source.Description),
 target.Payer_Name = TRIM(source.Payer_Name),
 target.Payer_Num = source.Payer_Num,
 target.Payer_Class = TRIM(source.Payer_Class),
 target.Payer_Type = TRIM(source.Payer_Type),
 target.Pymt_Type = TRIM(source.Pymt_Type),
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.Lock_By = TRIM(source.Lock_By),
 target.NonARGL = source.NonARGL,
 target.ERAPostNum = source.ERAPostNum,
 target.PaymentPK = TRIM(source.PaymentPK),
 target.PaymentPK_txt = TRIM(source.PaymentPK_txt),
 target.Crt_DateTime = source.Crt_DateTime,
 target.RegionKey = source.RegionKey,
 target.sysstarttime = source.sysstarttime,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Payment_Num, Batch_Num, Seq_Num, Category, Practice, Clinic, Check_Num, Check_Date, Check_Amt, Remain_Amt, Deposit_Date, Description, Payer_Name, Payer_Num, Payer_Class, Payer_Type, Pymt_Type, Last_Upd_UserID, Last_Upd_DateTime, Lock_By, NonARGL, ERAPostNum, PaymentPK, PaymentPK_txt, Crt_DateTime, RegionKey, sysstarttime, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (source.Payment_Num, source.Batch_Num, TRIM(source.Seq_Num), TRIM(source.Category), TRIM(source.Practice), TRIM(source.Clinic), TRIM(source.Check_Num), source.Check_Date, source.Check_Amt, source.Remain_Amt, source.Deposit_Date, TRIM(source.Description), TRIM(source.Payer_Name), source.Payer_Num, TRIM(source.Payer_Class), TRIM(source.Payer_Type), TRIM(source.Pymt_Type), TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, TRIM(source.Lock_By), source.NonARGL, source.ERAPostNum, TRIM(source.PaymentPK), TRIM(source.PaymentPK_txt), source.Crt_DateTime, source.RegionKey, source.sysstarttime, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PaymentPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgPayment
      GROUP BY PaymentPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgPayment');
ELSE
  COMMIT TRANSACTION;
END IF;
