
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimBill AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactClaimBill AS source
ON target.BillKey = source.BillKey
WHEN MATCHED THEN
  UPDATE SET
  target.BillKey = source.BillKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.CoidConfigurationKey = source.CoidConfigurationKey,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ClaimPayer1IplanKey = source.ClaimPayer1IplanKey,
 target.FacilityKey = source.FacilityKey,
 target.BillTypeKey = source.BillTypeKey,
 target.IplanKey = source.IplanKey,
 target.BillMessage = TRIM(source.BillMessage),
 target.BatchNumber = source.BatchNumber,
 target.BillDateKey = source.BillDateKey,
 target.BillTime = source.BillTime,
 target.PracticeKey = source.PracticeKey,
 target.PracticeName = TRIM(source.PracticeName),
 target.SourcePrimaryKeyValue = TRIM(source.SourcePrimaryKeyValue),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (BillKey, ClaimKey, ClaimNumber, RegionKey, Coid, CoidConfigurationKey, ServicingProviderKey, ClaimPayer1IplanKey, FacilityKey, BillTypeKey, IplanKey, BillMessage, BatchNumber, BillDateKey, BillTime, PracticeKey, PracticeName, SourcePrimaryKeyValue, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.BillKey, source.ClaimKey, source.ClaimNumber, source.RegionKey, TRIM(source.Coid), source.CoidConfigurationKey, source.ServicingProviderKey, source.ClaimPayer1IplanKey, source.FacilityKey, source.BillTypeKey, source.IplanKey, TRIM(source.BillMessage), source.BatchNumber, source.BillDateKey, source.BillTime, source.PracticeKey, TRIM(source.PracticeName), TRIM(source.SourcePrimaryKeyValue), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT BillKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactClaimBill
      GROUP BY BillKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactClaimBill');
ELSE
  COMMIT TRANSACTION;
END IF;
