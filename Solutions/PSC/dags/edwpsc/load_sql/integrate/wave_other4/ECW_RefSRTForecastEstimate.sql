
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefSRTForecastEstimate AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefSRTForecastEstimate AS source
ON target.ForecastEstimateKey = source.ForecastEstimateKey
WHEN MATCHED THEN
  UPDATE SET
  target.ForecastEstimateKey = source.ForecastEstimateKey,
 target.Years = source.Years,
 target.Months = TRIM(source.Months),
 target.PEDate = source.PEDate,
 target.ItemNumber = source.ItemNumber,
 target.Owners = TRIM(source.Owners),
 target.ControlNumber = source.ControlNumber,
 target.ValescoInd = TRIM(source.ValescoInd),
 target.OriginalItemNumber = source.OriginalItemNumber,
 target.COID = source.COID,
 target.Vendor = TRIM(source.Vendor),
 target.Notes = TRIM(source.Notes),
 target.Deptartment = source.Deptartment,
 target.InitialCMSRTEstimates = source.InitialCMSRTEstimates,
 target.FinalCMSRTEstimates = source.FinalCMSRTEstimates,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DeleteFlag = source.DeleteFlag,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ForecastEstimateKey, Years, Months, PEDate, ItemNumber, Owners, ControlNumber, ValescoInd, OriginalItemNumber, COID, Vendor, Notes, Deptartment, InitialCMSRTEstimates, FinalCMSRTEstimates, SourceAPrimaryKeyValue, DeleteFlag, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ForecastEstimateKey, source.Years, TRIM(source.Months), source.PEDate, source.ItemNumber, TRIM(source.Owners), source.ControlNumber, TRIM(source.ValescoInd), source.OriginalItemNumber, source.COID, TRIM(source.Vendor), TRIM(source.Notes), source.Deptartment, source.InitialCMSRTEstimates, source.FinalCMSRTEstimates, source.SourceAPrimaryKeyValue, source.DeleteFlag, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ForecastEstimateKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefSRTForecastEstimate
      GROUP BY ForecastEstimateKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefSRTForecastEstimate');
ELSE
  COMMIT TRANSACTION;
END IF;
