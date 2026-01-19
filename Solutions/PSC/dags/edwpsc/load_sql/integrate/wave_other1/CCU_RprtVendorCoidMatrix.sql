
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorCoidMatrix AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtVendorCoidMatrix AS source
ON target.VendorCoidMatrixKey = source.VendorCoidMatrixKey
WHEN MATCHED THEN
  UPDATE SET
  target.VendorCoidMatrixKey = source.VendorCoidMatrixKey,
 target.COID = TRIM(source.COID),
 target.COIDName = TRIM(source.COIDName),
 target.Active = source.Active,
 target.SystemValue = TRIM(source.SystemValue),
 target.VendorValue = TRIM(source.VendorValue),
 target.AssignedDate = source.AssignedDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (VendorCoidMatrixKey, COID, COIDName, Active, SystemValue, VendorValue, AssignedDate, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.VendorCoidMatrixKey, TRIM(source.COID), TRIM(source.COIDName), source.Active, TRIM(source.SystemValue), TRIM(source.VendorValue), source.AssignedDate, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT VendorCoidMatrixKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorCoidMatrix
      GROUP BY VendorCoidMatrixKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtVendorCoidMatrix');
ELSE
  COMMIT TRANSACTION;
END IF;
