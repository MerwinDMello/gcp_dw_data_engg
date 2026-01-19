
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactUacDetail AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactUacDetail AS source
ON target.UacDetailKey = source.UacDetailKey
WHEN MATCHED THEN
  UPDATE SET
  target.UacDetailKey = source.UacDetailKey,
 target.Practice = TRIM(source.Practice),
 target.PaymentNumber = source.PaymentNumber,
 target.TransactionDate = source.TransactionDate,
 target.TransactionType = TRIM(source.TransactionType),
 target.TransactionAmount = source.TransactionAmount,
 target.TransactionDescription = TRIM(source.TransactionDescription),
 target.DBAccount = TRIM(source.DBAccount),
 target.CRAccount = TRIM(source.CRAccount),
 target.PKCreatedUserID = TRIM(source.PKCreatedUserID),
 target.PKCreatedDateTime = source.PKCreatedDateTime,
 target.UacDetailPK = TRIM(source.UacDetailPK),
 target.OrgUacNumber = source.OrgUacNumber,
 target.RegionKey = source.RegionKey,
 target.SourceAPrimaryKey = source.SourceAPrimaryKey,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDatetime = source.DWLastUpdateDatetime
WHEN NOT MATCHED THEN
  INSERT (UacDetailKey, Practice, PaymentNumber, TransactionDate, TransactionType, TransactionAmount, TransactionDescription, DBAccount, CRAccount, PKCreatedUserID, PKCreatedDateTime, UacDetailPK, OrgUacNumber, RegionKey, SourceAPrimaryKey, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDatetime)
  VALUES (source.UacDetailKey, TRIM(source.Practice), source.PaymentNumber, source.TransactionDate, TRIM(source.TransactionType), source.TransactionAmount, TRIM(source.TransactionDescription), TRIM(source.DBAccount), TRIM(source.CRAccount), TRIM(source.PKCreatedUserID), source.PKCreatedDateTime, TRIM(source.UacDetailPK), source.OrgUacNumber, source.RegionKey, source.SourceAPrimaryKey, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDatetime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT UacDetailKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactUacDetail
      GROUP BY UacDetailKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactUacDetail');
ELSE
  COMMIT TRANSACTION;
END IF;
