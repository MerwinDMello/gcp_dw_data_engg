
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineChargeUserChangeDate AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimLineChargeUserChangeDate AS source
ON target.ClaimLineChargeUserChangeDateKey = source.ClaimLineChargeUserChangeDateKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimLineChargeUserChangeDateKey = source.ClaimLineChargeUserChangeDateKey,
 target.ClaimLineChargeKey = source.ClaimLineChargeKey,
 target.SourceSystemType = TRIM(source.SourceSystemType),
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_Datetime = source.Crt_Datetime,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.Payer_Num = source.Payer_Num,
 target.Crg_Balance = source.Crg_Balance,
 target.Crg_Amt = source.Crg_Amt,
 target.Proc_Code = TRIM(source.Proc_Code),
 target.Modifier = TRIM(source.Modifier),
 target.Quantity = source.Quantity,
 target.CPTDeleted = TRIM(source.CPTDeleted),
 target.RegionKey = source.RegionKey,
 target.sysstarttime = source.sysstarttime,
 target.sysendtime = source.sysendtime,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimLineChargeUserChangeDateKey, ClaimLineChargeKey, SourceSystemType, Crt_UserID, Crt_Datetime, Last_Upd_UserID, Last_Upd_DateTime, Payer_Num, Crg_Balance, Crg_Amt, Proc_Code, Modifier, Quantity, CPTDeleted, RegionKey, sysstarttime, sysendtime, SourceAPrimaryKeyValue, SourceBPrimaryKeyValue, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimLineChargeUserChangeDateKey, source.ClaimLineChargeKey, TRIM(source.SourceSystemType), TRIM(source.Crt_UserID), source.Crt_Datetime, TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, source.Payer_Num, source.Crg_Balance, source.Crg_Amt, TRIM(source.Proc_Code), TRIM(source.Modifier), source.Quantity, TRIM(source.CPTDeleted), source.RegionKey, source.sysstarttime, source.sysendtime, TRIM(source.SourceAPrimaryKeyValue), TRIM(source.SourceBPrimaryKeyValue), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimLineChargeUserChangeDateKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineChargeUserChangeDate
      GROUP BY ClaimLineChargeUserChangeDateKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactClaimLineChargeUserChangeDate');
ELSE
  COMMIT TRANSACTION;
END IF;
