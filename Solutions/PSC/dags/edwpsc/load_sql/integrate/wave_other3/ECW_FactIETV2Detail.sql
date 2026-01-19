
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2Detail AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactIETV2Detail AS source
ON target.IETV2DetailKey = source.IETV2DetailKey
WHEN MATCHED THEN
  UPDATE SET
  target.IETV2DetailKey = source.IETV2DetailKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.COID = TRIM(source.COID),
 target.AgeInDays = source.AgeInDays,
 target.Resolved = source.Resolved,
 target.InstructionsAdditionalDetail = TRIM(source.InstructionsAdditionalDetail),
 target.TYPE = TRIM(source.TYPE),
 target.ResolvedBy = TRIM(source.ResolvedBy),
 target.ResolvedDateKey = source.ResolvedDateKey,
 target.ResolvedTime = source.ResolvedTime,
 target.Resolution = TRIM(source.Resolution),
 target.ClaimCaseCreateDateKey = source.ClaimCaseCreateDateKey,
 target.ClaimCaseCreateTime = source.ClaimCaseCreateTime,
 target.DaysAtPractice = source.DaysAtPractice,
 target.FieldName = TRIM(source.FieldName),
 target.LastModifiedBy = TRIM(source.LastModifiedBy),
 target.LastModifiedDateKey = source.LastModifiedDateKey,
 target.LastModifiedTime = source.LastModifiedTime,
 target.IsManual = source.IsManual,
 target.PayerName = TRIM(source.PayerName),
 target.ReportName = TRIM(source.ReportName),
 target.ErrorNumber = source.ErrorNumber,
 target.SubcategoryName = TRIM(source.SubcategoryName),
 target.ErrorMessage = TRIM(source.ErrorMessage),
 target.PlaceOfServiceCode = TRIM(source.PlaceOfServiceCode),
 target.TotalBalance = source.TotalBalance,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ClaimErrorTypeDescription = TRIM(source.ClaimErrorTypeDescription),
 target.Deleted = source.Deleted,
 target.Closurecategory = TRIM(source.Closurecategory),
 target.Latestquestiondate = source.Latestquestiondate,
 target.Latestanswerdate = source.Latestanswerdate,
 target.Closeddate = source.Closeddate,
 target.Isatcbo = source.Isatcbo,
 target.Sourcedepartment = TRIM(source.Sourcedepartment),
 target.LatestQuestion = TRIM(source.LatestQuestion),
 target.LatestAnswer = TRIM(source.LatestAnswer),
 target.ClaimQueryId = TRIM(source.ClaimQueryId),
 target.ClaimCaseId = TRIM(source.ClaimCaseId),
 target.ClaimAction = TRIM(source.ClaimAction),
 target.SubcategoryID = source.SubcategoryID,
 target.ClaimStatus = TRIM(source.ClaimStatus),
 target.CPTCode = TRIM(source.CPTCode),
 target.CreatedBy = TRIM(source.CreatedBy),
 target.SubCategoryOriginId = TRIM(source.SubCategoryOriginId),
 target.CategoryName = TRIM(source.CategoryName),
 target.FullClaimNumber = TRIM(source.FullClaimNumber)
WHEN NOT MATCHED THEN
  INSERT (IETV2DetailKey, ClaimKey, ClaimNumber, COID, AgeInDays, Resolved, InstructionsAdditionalDetail, TYPE, ResolvedBy, ResolvedDateKey, ResolvedTime, Resolution, ClaimCaseCreateDateKey, ClaimCaseCreateTime, DaysAtPractice, FieldName, LastModifiedBy, LastModifiedDateKey, LastModifiedTime, IsManual, PayerName, ReportName, ErrorNumber, SubcategoryName, ErrorMessage, PlaceOfServiceCode, TotalBalance, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ClaimErrorTypeDescription, Deleted, Closurecategory, Latestquestiondate, Latestanswerdate, Closeddate, Isatcbo, Sourcedepartment, LatestQuestion, LatestAnswer, ClaimQueryId, ClaimCaseId, ClaimAction, SubcategoryID, ClaimStatus, CPTCode, CreatedBy, SubCategoryOriginId, CategoryName, FullClaimNumber)
  VALUES (source.IETV2DetailKey, source.ClaimKey, source.ClaimNumber, TRIM(source.COID), source.AgeInDays, source.Resolved, TRIM(source.InstructionsAdditionalDetail), TRIM(source.TYPE), TRIM(source.ResolvedBy), source.ResolvedDateKey, source.ResolvedTime, TRIM(source.Resolution), source.ClaimCaseCreateDateKey, source.ClaimCaseCreateTime, source.DaysAtPractice, TRIM(source.FieldName), TRIM(source.LastModifiedBy), source.LastModifiedDateKey, source.LastModifiedTime, source.IsManual, TRIM(source.PayerName), TRIM(source.ReportName), source.ErrorNumber, TRIM(source.SubcategoryName), TRIM(source.ErrorMessage), TRIM(source.PlaceOfServiceCode), source.TotalBalance, source.SourcePrimaryKeyValue, source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.ClaimErrorTypeDescription), source.Deleted, TRIM(source.Closurecategory), source.Latestquestiondate, source.Latestanswerdate, source.Closeddate, source.Isatcbo, TRIM(source.Sourcedepartment), TRIM(source.LatestQuestion), TRIM(source.LatestAnswer), TRIM(source.ClaimQueryId), TRIM(source.ClaimCaseId), TRIM(source.ClaimAction), source.SubcategoryID, TRIM(source.ClaimStatus), TRIM(source.CPTCode), TRIM(source.CreatedBy), TRIM(source.SubCategoryOriginId), TRIM(source.CategoryName), TRIM(source.FullClaimNumber));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IETV2DetailKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2Detail
      GROUP BY IETV2DetailKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactIETV2Detail');
ELSE
  COMMIT TRANSACTION;
END IF;
