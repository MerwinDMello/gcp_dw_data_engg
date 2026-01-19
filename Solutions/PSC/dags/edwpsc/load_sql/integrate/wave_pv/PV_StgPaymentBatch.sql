
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgPaymentBatch AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgPaymentBatch AS source
ON target.PaymentBatchPK = source.PaymentBatchPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Batch_Num = source.Batch_Num,
 target.Practice = TRIM(source.Practice),
 target.Clinic = TRIM(source.Clinic),
 target.Deposit_Date = source.Deposit_Date,
 target.Category = TRIM(source.Category),
 target.Total_Amt = source.Total_Amt,
 target.Remain_Amt = source.Remain_Amt,
 target.Description = TRIM(source.Description),
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Lock_By = TRIM(source.Lock_By),
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.ERAPostNum = source.ERAPostNum,
 target.PaymentBatchPK = TRIM(source.PaymentBatchPK),
 target.Crt_DateTime = source.Crt_DateTime,
 target.RegionKey = source.RegionKey,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (Batch_Num, Practice, Clinic, Deposit_Date, Category, Total_Amt, Remain_Amt, Description, Crt_UserID, Lock_By, Last_Upd_UserID, Last_Upd_DateTime, ERAPostNum, PaymentBatchPK, Crt_DateTime, RegionKey, SourcePhysicalDeleteFlag, DWLastUpdateDateTime)
  VALUES (source.Batch_Num, TRIM(source.Practice), TRIM(source.Clinic), source.Deposit_Date, TRIM(source.Category), source.Total_Amt, source.Remain_Amt, TRIM(source.Description), TRIM(source.Crt_UserID), TRIM(source.Lock_By), TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, source.ERAPostNum, TRIM(source.PaymentBatchPK), source.Crt_DateTime, source.RegionKey, source.SourcePhysicalDeleteFlag, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PaymentBatchPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgPaymentBatch
      GROUP BY PaymentBatchPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgPaymentBatch');
ELSE
  COMMIT TRANSACTION;
END IF;
