
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgInvTrans AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgInvTrans AS source
ON target.InvTransPK = source.InvTransPK
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Inv_Num = source.Inv_Num,
 target.Line_Num = source.Line_Num,
 target.Trans_Num = source.Trans_Num,
 target.Trans_Date = source.Trans_Date,
 target.Trans_Type = TRIM(source.Trans_Type),
 target.Trans_Amt = source.Trans_Amt,
 target.Trans_Desc = TRIM(source.Trans_Desc),
 target.Reason = TRIM(source.Reason),
 target.Payment_Num = source.Payment_Num,
 target.DB_Acnt = TRIM(source.DB_Acnt),
 target.CR_Acnt = TRIM(source.CR_Acnt),
 target.Closing_Date = source.Closing_Date,
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_DateTime = source.Crt_DateTime,
 target.InvTransPK = TRIM(source.InvTransPK),
 target.RegionKey = source.RegionKey,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (Practice, Inv_Num, Line_Num, Trans_Num, Trans_Date, Trans_Type, Trans_Amt, Trans_Desc, Reason, Payment_Num, DB_Acnt, CR_Acnt, Closing_Date, Crt_UserID, Crt_DateTime, InvTransPK, RegionKey, SourcePhysicalDeleteFlag, DWLastUpdateDateTime)
  VALUES (TRIM(source.Practice), source.Inv_Num, source.Line_Num, source.Trans_Num, source.Trans_Date, TRIM(source.Trans_Type), source.Trans_Amt, TRIM(source.Trans_Desc), TRIM(source.Reason), source.Payment_Num, TRIM(source.DB_Acnt), TRIM(source.CR_Acnt), source.Closing_Date, TRIM(source.Crt_UserID), source.Crt_DateTime, TRIM(source.InvTransPK), source.RegionKey, source.SourcePhysicalDeleteFlag, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT InvTransPK
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgInvTrans
      GROUP BY InvTransPK
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgInvTrans');
ELSE
  COMMIT TRANSACTION;
END IF;
