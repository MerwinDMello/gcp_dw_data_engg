
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgUacDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgUacDetail AS source
ON target.UacDetailPK = source.UacDetailPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.UacDetail_Num = source.UacDetail_Num,
 target.Payment_Num = source.Payment_Num,
 target.Trans_Date = source.Trans_Date,
 target.Trans_Type = TRIM(source.Trans_Type),
 target.Trans_Amt = source.Trans_Amt,
 target.Trans_Desc = TRIM(source.Trans_Desc),
 target.DB_Acnt = TRIM(source.DB_Acnt),
 target.CR_Acnt = TRIM(source.CR_Acnt),
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_DateTime = source.Crt_DateTime,
 target.UacDetailPK = TRIM(source.UacDetailPK),
 target.Org_Uac_Num = source.Org_Uac_Num,
 target.RegionKey = source.RegionKey,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Practice, UacDetail_Num, Payment_Num, Trans_Date, Trans_Type, Trans_Amt, Trans_Desc, DB_Acnt, CR_Acnt, Crt_UserID, Crt_DateTime, UacDetailPK, Org_Uac_Num, RegionKey, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Practice), source.UacDetail_Num, source.Payment_Num, source.Trans_Date, TRIM(source.Trans_Type), source.Trans_Amt, TRIM(source.Trans_Desc), TRIM(source.DB_Acnt), TRIM(source.CR_Acnt), TRIM(source.Crt_UserID), source.Crt_DateTime, TRIM(source.UacDetailPK), source.Org_Uac_Num, source.RegionKey, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UacDetailPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgUacDetail
      GROUP BY UacDetailPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgUacDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
