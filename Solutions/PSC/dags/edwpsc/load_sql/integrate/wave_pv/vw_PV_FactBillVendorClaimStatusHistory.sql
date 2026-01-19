
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.vw_PV_FactBillVendorClaimStatusHistory AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactBillVendorClaimStatusHistory AS source
ON target.BillVendorClaimStatusHistoryKey = source.BillVendorClaimStatusHistoryKey
WHEN MATCHED THEN
  UPDATE SET
  target.BillVendorClaimStatusHistoryKey = source.BillVendorClaimStatusHistoryKey,
 target.SourcePrimaryKeyValue = source.SourcePrimaryKeyValue,
 target.ClaimNumber = source.ClaimNumber,
 target.ClaimKey = source.ClaimKey,
 target.TraceNumber = TRIM(source.TraceNumber),
 target.PatientICN = TRIM(source.PatientICN),
 target.ClaimTotal = source.ClaimTotal,
 target.Category277 = TRIM(source.Category277),
 target.Status277 = TRIM(source.Status277),
 target.StatDate = source.StatDate,
 target.ReportType = TRIM(source.ReportType),
 target.ProcessDate = source.ProcessDate,
 target.PayID = TRIM(source.PayID),
 target.PaySubId = TRIM(source.PaySubId),
 target.ClaimRank = source.ClaimRank,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.TrackStat = TRIM(source.TrackStat),
 target.AddStatus = TRIM(source.AddStatus),
 target.DelChrgLoop = TRIM(source.DelChrgLoop),
 target.COID = TRIM(source.COID)
WHEN NOT MATCHED THEN
  INSERT (BillVendorClaimStatusHistoryKey, SourcePrimaryKeyValue, ClaimNumber, ClaimKey, TraceNumber, PatientICN, ClaimTotal, Category277, Status277, StatDate, ReportType, ProcessDate, PayID, PaySubId, ClaimRank, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, TrackStat, AddStatus, DelChrgLoop, COID)
  VALUES (source.BillVendorClaimStatusHistoryKey, source.SourcePrimaryKeyValue, source.ClaimNumber, source.ClaimKey, TRIM(source.TraceNumber), TRIM(source.PatientICN), source.ClaimTotal, TRIM(source.Category277), TRIM(source.Status277), source.StatDate, TRIM(source.ReportType), source.ProcessDate, TRIM(source.PayID), TRIM(source.PaySubId), source.ClaimRank, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.TrackStat), TRIM(source.AddStatus), TRIM(source.DelChrgLoop), TRIM(source.COID));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BillVendorClaimStatusHistoryKey
      FROM {{ params.param_psc_core_dataset_name }}.vw_PV_FactBillVendorClaimStatusHistory
      GROUP BY BillVendorClaimStatusHistoryKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.vw_PV_FactBillVendorClaimStatusHistory');
ELSE
  COMMIT TRANSACTION;
END IF;
