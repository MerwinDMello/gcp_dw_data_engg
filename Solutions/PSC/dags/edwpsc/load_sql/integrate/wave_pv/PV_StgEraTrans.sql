
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgEraTrans AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgEraTrans AS source
ON target.EraTransPK = source.EraTransPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.ERA_Date = source.ERA_Date,
 target.ERA_Num = source.ERA_Num,
 target.Receiver_Name = TRIM(source.Receiver_Name),
 target.Receiver_ID = TRIM(source.Receiver_ID),
 target.Sender_Name = TRIM(source.Sender_Name),
 target.Sender_ID = TRIM(source.Sender_ID),
 target.ISA_Ctrl_Num = TRIM(source.ISA_Ctrl_Num),
 target.Environment = TRIM(source.Environment),
 target.Check_Nums = TRIM(source.Check_Nums),
 target.ERA_Post_Date = source.ERA_Post_Date,
 target.Parsed_Report = TRIM(source.Parsed_Report),
 target.Posted_Report = TRIM(source.Posted_Report),
 target.Exception_Report = TRIM(source.Exception_Report),
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.EraTransPK = TRIM(source.EraTransPK),
 target.DocRootID = source.DocRootID,
 target.DocPath = TRIM(source.DocPath),
 target.ERAFileName = TRIM(source.ERAFileName),
 target.RegionKey = source.RegionKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Practice, ERA_Date, ERA_Num, Receiver_Name, Receiver_ID, Sender_Name, Sender_ID, ISA_Ctrl_Num, Environment, Check_Nums, ERA_Post_Date, Parsed_Report, Posted_Report, Exception_Report, Last_Upd_UserID, Last_Upd_DateTime, EraTransPK, DocRootID, DocPath, ERAFileName, RegionKey, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Practice), source.ERA_Date, source.ERA_Num, TRIM(source.Receiver_Name), TRIM(source.Receiver_ID), TRIM(source.Sender_Name), TRIM(source.Sender_ID), TRIM(source.ISA_Ctrl_Num), TRIM(source.Environment), TRIM(source.Check_Nums), source.ERA_Post_Date, TRIM(source.Parsed_Report), TRIM(source.Posted_Report), TRIM(source.Exception_Report), TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, TRIM(source.EraTransPK), source.DocRootID, TRIM(source.DocPath), TRIM(source.ERAFileName), source.RegionKey, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EraTransPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgEraTrans
      GROUP BY EraTransPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgEraTrans');
ELSE
  COMMIT TRANSACTION;
END IF;
