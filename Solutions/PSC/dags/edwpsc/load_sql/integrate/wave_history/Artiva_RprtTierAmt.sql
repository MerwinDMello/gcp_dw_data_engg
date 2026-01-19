
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_RprtTierAmt AS target
USING {{ params.param_psc_stage_dataset_name }}.Artiva_RprtTierAmt AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.SnapShotDate = source.SnapShotDate,
 target.SystemName = TRIM(source.SystemName),
 target.PatientAccountingNumber = TRIM(source.PatientAccountingNumber),
 target.ECWClaimKey = source.ECWClaimKey,
 target.ECWClaimNumber = source.ECWClaimNumber,
 target.ECWRegionKey = source.ECWRegionKey,
 target.PVClaimKey = source.PVClaimKey,
 target.PVClaimNumber = source.PVClaimNumber,
 target.PVRegionKey = source.PVRegionKey,
 target.ArtivaHCENID = TRIM(source.ArtivaHCENID),
 target.ArtivaBalanceAmt = source.ArtivaBalanceAmt,
 target.ArtivaPrimaryFinancialClass = source.ArtivaPrimaryFinancialClass,
 target.ArtivaLastLoadDate = source.ArtivaLastLoadDate,
 target.ArtivaLastWorkedDate = source.ArtivaLastWorkedDate,
 target.ArtivaCOID = TRIM(source.ArtivaCOID),
 target.ArtivaServiceDateKey = source.ArtivaServiceDateKey,
 target.ArtivaFinalBillDateKey = source.ArtivaFinalBillDateKey,
 target.ArtivaOriginalBalance = source.ArtivaOriginalBalance,
 target.ArtivaTotalAdjustments = source.ArtivaTotalAdjustments,
 target.ArtivaTotalPayments = source.ArtivaTotalPayments,
 target.ArtivaTotalCharges = source.ArtivaTotalCharges,
 target.ArtivaTierAmt = source.ArtivaTierAmt,
 target.SystemARClaimFlag = source.SystemARClaimFlag,
 target.SystemTotalBalanceAmt = source.SystemTotalBalanceAmt,
 target.SystemVoidFlag = source.SystemVoidFlag,
 target.SystemDeleteFlag = source.SystemDeleteFlag,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapShotDate, SystemName, PatientAccountingNumber, ECWClaimKey, ECWClaimNumber, ECWRegionKey, PVClaimKey, PVClaimNumber, PVRegionKey, ArtivaHCENID, ArtivaBalanceAmt, ArtivaPrimaryFinancialClass, ArtivaLastLoadDate, ArtivaLastWorkedDate, ArtivaCOID, ArtivaServiceDateKey, ArtivaFinalBillDateKey, ArtivaOriginalBalance, ArtivaTotalAdjustments, ArtivaTotalPayments, ArtivaTotalCharges, ArtivaTierAmt, SystemARClaimFlag, SystemTotalBalanceAmt, SystemVoidFlag, SystemDeleteFlag, InsertedBy, InsertedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapShotDate, TRIM(source.SystemName), TRIM(source.PatientAccountingNumber), source.ECWClaimKey, source.ECWClaimNumber, source.ECWRegionKey, source.PVClaimKey, source.PVClaimNumber, source.PVRegionKey, TRIM(source.ArtivaHCENID), source.ArtivaBalanceAmt, source.ArtivaPrimaryFinancialClass, source.ArtivaLastLoadDate, source.ArtivaLastWorkedDate, TRIM(source.ArtivaCOID), source.ArtivaServiceDateKey, source.ArtivaFinalBillDateKey, source.ArtivaOriginalBalance, source.ArtivaTotalAdjustments, source.ArtivaTotalPayments, source.ArtivaTotalCharges, source.ArtivaTierAmt, source.SystemARClaimFlag, source.SystemTotalBalanceAmt, source.SystemVoidFlag, source.SystemDeleteFlag, TRIM(source.InsertedBy), source.InsertedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.Artiva_RprtTierAmt
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_RprtTierAmt');
ELSE
  COMMIT TRANSACTION;
END IF;
