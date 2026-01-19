
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactVendorProductivity AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactVendorProductivity AS source
ON target.VendorProductivityKey = source.VendorProductivityKey
WHEN MATCHED THEN
  UPDATE SET
  target.ProductivityType = TRIM(source.ProductivityType),
 target.WorkQueueCID = source.WorkQueueCID,
 target.WorkQueueID = source.WorkQueueID,
 target.WorkQueueName = TRIM(source.WorkQueueName),
 target.RecordCID = source.RecordCID,
 target.RecordInternalID = source.RecordInternalID,
 target.Activity = source.Activity,
 target.ActivityName = TRIM(source.ActivityName),
 target.EpicUserID = TRIM(source.EpicUserID),
 target.User34 = TRIM(source.User34),
 target.UserName = TRIM(source.UserName),
 target.ActivityDate = source.ActivityDate,
 target.ActivityTime = source.ActivityTime,
 target.InvoiceNumber = TRIM(source.InvoiceNumber),
 target.EpicAccountID = source.EpicAccountID,
 target.GuarantorID = source.GuarantorID,
 target.GuarantorName = TRIM(source.GuarantorName),
 target.BillCID = source.BillCID,
 target.BillAreaID = source.BillAreaID,
 target.ServiceDate = source.ServiceDate,
 target.RegionKey = source.RegionKey,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.VendorProductivityKey = source.VendorProductivityKey
WHEN NOT MATCHED THEN
  INSERT (ProductivityType, WorkQueueCID, WorkQueueID, WorkQueueName, RecordCID, RecordInternalID, Activity, ActivityName, EpicUserID, User34, UserName, ActivityDate, ActivityTime, InvoiceNumber, EpicAccountID, GuarantorID, GuarantorName, BillCID, BillAreaID, ServiceDate, RegionKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM,VendorProductivityKey)
  VALUES (TRIM(source.ProductivityType), source.WorkQueueCID, source.WorkQueueID, TRIM(source.WorkQueueName), source.RecordCID, source.RecordInternalID, source.Activity, TRIM(source.ActivityName), TRIM(source.EpicUserID), TRIM(source.User34), TRIM(source.UserName), source.ActivityDate, source.ActivityTime, TRIM(source.InvoiceNumber), source.EpicAccountID, source.GuarantorID, TRIM(source.GuarantorName), source.BillCID, source.BillAreaID, source.ServiceDate, source.RegionKey, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM,source.VendorProductivityKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT VendorProductivityKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactVendorProductivity
      GROUP BY VendorProductivityKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactVendorProductivity');
ELSE
  COMMIT TRANSACTION;
END IF;
