
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSRTDiscountTracker AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSRTDiscountTracker AS source
ON target.SRTDiscountTrackerKey = source.SRTDiscountTrackerKey
WHEN MATCHED THEN
  UPDATE SET
  target.SRTDiscountTrackerKey = source.SRTDiscountTrackerKey,
 target.ControlNumber = TRIM(source.ControlNumber),
 target.Partners = TRIM(source.Partners),
 target.DiscountPeriod = source.DiscountPeriod,
 target.InvoiceDescription = TRIM(source.InvoiceDescription),
 target.Department = TRIM(source.Department),
 target.Owners = TRIM(source.Owners),
 target.InitialQuality = TRIM(source.InitialQuality),
 target.FinalAuditScore = source.FinalAuditScore,
 target.InvoiceTotal = source.InvoiceTotal,
 target.TotalDiscountPercent = source.TotalDiscountPercent,
 target.TotalDiscountAmount = source.TotalDiscountAmount,
 target.QualityDiscountPercent = source.QualityDiscountPercent,
 target.QADiscountApplied = source.QADiscountApplied,
 target.SLADiscountPercent = source.SLADiscountPercent,
 target.SLADiscountApplied = source.SLADiscountApplied,
 target.BilledUnits = source.BilledUnits,
 target.ActualUnits = source.ActualUnits,
 target.UnitsNotWorked = source.UnitsNotWorked,
 target.InvoiceDiscrepancyIdentified = source.InvoiceDiscrepancyIdentified,
 target.InvoiceDiscrepancyAmount = source.InvoiceDiscrepancyAmount,
 target.Rate = source.Rate,
 target.InvoicetoApplyDiscount = source.InvoicetoApplyDiscount,
 target.NotificationSent = source.NotificationSent,
 target.FinalAmountDue = source.FinalAmountDue,
 target.TotalReceivedAmount = source.TotalReceivedAmount,
 target.FinalReceivedMonth = source.FinalReceivedMonth,
 target.DiscountReceived = TRIM(source.DiscountReceived),
 target.DiscountUnderOverapplied = source.DiscountUnderOverapplied,
 target.InvoiceCorrectionsRequired = TRIM(source.InvoiceCorrectionsRequired),
 target.DiscountType = TRIM(source.DiscountType),
 target.PoolCompletionTier = TRIM(source.PoolCompletionTier),
 target.AccountsPresentedNum = source.AccountsPresentedNum,
 target.AccountsWorkedNum = source.AccountsWorkedNum,
 target.AccountsMissedforPoolCompletion = source.AccountsMissedforPoolCompletion,
 target.DiscountTrackerKey = source.DiscountTrackerKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DeleteFlag = source.DeleteFlag
WHEN NOT MATCHED THEN
  INSERT (SRTDiscountTrackerKey, ControlNumber, Partners, DiscountPeriod, InvoiceDescription, Department, Owners, InitialQuality, FinalAuditScore, InvoiceTotal, TotalDiscountPercent, TotalDiscountAmount, QualityDiscountPercent, QADiscountApplied, SLADiscountPercent, SLADiscountApplied, BilledUnits, ActualUnits, UnitsNotWorked, InvoiceDiscrepancyIdentified, InvoiceDiscrepancyAmount, Rate, InvoicetoApplyDiscount, NotificationSent, FinalAmountDue, TotalReceivedAmount, FinalReceivedMonth, DiscountReceived, DiscountUnderOverapplied, InvoiceCorrectionsRequired, DiscountType, PoolCompletionTier, AccountsPresentedNum, AccountsWorkedNum, AccountsMissedforPoolCompletion, DiscountTrackerKey, SourcePrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DeleteFlag)
  VALUES (source.SRTDiscountTrackerKey, TRIM(source.ControlNumber), TRIM(source.Partners), source.DiscountPeriod, TRIM(source.InvoiceDescription), TRIM(source.Department), TRIM(source.Owners), TRIM(source.InitialQuality), source.FinalAuditScore, source.InvoiceTotal, source.TotalDiscountPercent, source.TotalDiscountAmount, source.QualityDiscountPercent, source.QADiscountApplied, source.SLADiscountPercent, source.SLADiscountApplied, source.BilledUnits, source.ActualUnits, source.UnitsNotWorked, source.InvoiceDiscrepancyIdentified, source.InvoiceDiscrepancyAmount, source.Rate, source.InvoicetoApplyDiscount, source.NotificationSent, source.FinalAmountDue, source.TotalReceivedAmount, source.FinalReceivedMonth, TRIM(source.DiscountReceived), source.DiscountUnderOverapplied, TRIM(source.InvoiceCorrectionsRequired), TRIM(source.DiscountType), TRIM(source.PoolCompletionTier), source.AccountsPresentedNum, source.AccountsWorkedNum, source.AccountsMissedforPoolCompletion, source.DiscountTrackerKey, source.SourcePrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SRTDiscountTrackerKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSRTDiscountTracker
      GROUP BY SRTDiscountTrackerKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSRTDiscountTracker');
ELSE
  COMMIT TRANSACTION;
END IF;
