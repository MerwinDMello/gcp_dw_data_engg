
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgEraService AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgEraService AS source
ON target.EraServicePK = source.EraServicePK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.ERA_Date = source.ERA_Date,
 target.ERA_Num = source.ERA_Num,
 target.ST_Ctrl_Num = TRIM(source.ST_Ctrl_Num),
 target.Pvd_Num = TRIM(source.Pvd_Num),
 target.Claim_Num = TRIM(source.Claim_Num),
 target.Svc_Code = TRIM(source.Svc_Code),
 target.Modifier = TRIM(source.Modifier),
 target.Ctrl_Num = source.Ctrl_Num,
 target.Line_Num = source.Line_Num,
 target.Claim_Status = source.Claim_Status,
 target.Inv_Num = source.Inv_Num,
 target.Proc_Code = TRIM(source.Proc_Code),
 target.Ar_Ctrl_Num = source.Ar_Ctrl_Num,
 target.Modifier1 = TRIM(source.Modifier1),
 target.Modifier2 = TRIM(source.Modifier2),
 target.Modifier3 = TRIM(source.Modifier3),
 target.Svc_Date = source.Svc_Date,
 target.Pos_Code = TRIM(source.Pos_Code),
 target.Rndr_Pvd_ID = TRIM(source.Rndr_Pvd_ID),
 target.Status = TRIM(source.Status),
 target.Charge = source.Charge,
 target.Expected = source.Expected,
 target.Covered = source.Covered,
 target.Deductible = source.Deductible,
 target.Copay = source.Copay,
 target.Paid_To_Patient = source.Paid_To_Patient,
 target.Payment = source.Payment,
 target.NonCovered = source.NonCovered,
 target.Payment_Posted = source.Payment_Posted,
 target.Adj_Posted = source.Adj_Posted,
 target.Deduct_Posted = source.Deduct_Posted,
 target.Increased_AR = source.Increased_AR,
 target.Balance = source.Balance,
 target.Reason = TRIM(source.Reason),
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.EraServicePK = TRIM(source.EraServicePK),
 target.RegionKey = source.RegionKey,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (Practice, ERA_Date, ERA_Num, ST_Ctrl_Num, Pvd_Num, Claim_Num, Svc_Code, Modifier, Ctrl_Num, Line_Num, Claim_Status, Inv_Num, Proc_Code, Ar_Ctrl_Num, Modifier1, Modifier2, Modifier3, Svc_Date, Pos_Code, Rndr_Pvd_ID, Status, Charge, Expected, Covered, Deductible, Copay, Paid_To_Patient, Payment, NonCovered, Payment_Posted, Adj_Posted, Deduct_Posted, Increased_AR, Balance, Reason, Last_Upd_UserID, Last_Upd_DateTime, EraServicePK, RegionKey, SourcePhysicalDeleteFlag, DWLastUpdateDateTime)
  VALUES (TRIM(source.Practice), source.ERA_Date, source.ERA_Num, TRIM(source.ST_Ctrl_Num), TRIM(source.Pvd_Num), TRIM(source.Claim_Num), TRIM(source.Svc_Code), TRIM(source.Modifier), source.Ctrl_Num, source.Line_Num, source.Claim_Status, source.Inv_Num, TRIM(source.Proc_Code), source.Ar_Ctrl_Num, TRIM(source.Modifier1), TRIM(source.Modifier2), TRIM(source.Modifier3), source.Svc_Date, TRIM(source.Pos_Code), TRIM(source.Rndr_Pvd_ID), TRIM(source.Status), source.Charge, source.Expected, source.Covered, source.Deductible, source.Copay, source.Paid_To_Patient, source.Payment, source.NonCovered, source.Payment_Posted, source.Adj_Posted, source.Deduct_Posted, source.Increased_AR, source.Balance, TRIM(source.Reason), TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, TRIM(source.EraServicePK), source.RegionKey, source.SourcePhysicalDeleteFlag, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT EraServicePK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgEraService
      GROUP BY EraServicePK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgEraService');
ELSE
  COMMIT TRANSACTION;
END IF;
