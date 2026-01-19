
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactPVInventoryProductivity AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactPVInventoryProductivity AS source
ON target.CCUPVInventoryProductivityKey = source.CCUPVInventoryProductivityKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUPVInventoryProductivityKey = source.CCUPVInventoryProductivityKey,
 target.ProductivityDate = source.ProductivityDate,
 target.User34 = TRIM(source.User34),
 target.RegionKey = source.RegionKey,
 target.ClaimNumber = TRIM(source.ClaimNumber),
 target.Practice = TRIM(source.Practice),
 target.ClaimStatusChangedTo = TRIM(source.ClaimStatusChangedTo),
 target.ActionTime = source.ActionTime,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (CCUPVInventoryProductivityKey, ProductivityDate, User34, RegionKey, ClaimNumber, Practice, ClaimStatusChangedTo, ActionTime, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.CCUPVInventoryProductivityKey, source.ProductivityDate, TRIM(source.User34), source.RegionKey, TRIM(source.ClaimNumber), TRIM(source.Practice), TRIM(source.ClaimStatusChangedTo), source.ActionTime, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUPVInventoryProductivityKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactPVInventoryProductivity
      GROUP BY CCUPVInventoryProductivityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactPVInventoryProductivity');
ELSE
  COMMIT TRANSACTION;
END IF;
