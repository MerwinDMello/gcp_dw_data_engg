
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimPayer AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactClaimPayer AS source
ON target.ClaimPayerKey = source.ClaimPayerKey
WHEN MATCHED THEN
  UPDATE SET
  target.ClaimPayerKey = source.ClaimPayerKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.RegionKey = source.RegionKey,
 target.Coid = TRIM(source.Coid),
 target.SeqNumber = source.SeqNumber,
 target.PayerIplanKey = source.PayerIplanKey,
 target.PayerGroupNumber = TRIM(source.PayerGroupNumber),
 target.PayerGroupName = TRIM(source.PayerGroupName),
 target.PayerSubscriberNumber = TRIM(source.PayerSubscriberNumber),
 target.PayerClaimIndicator = TRIM(source.PayerClaimIndicator),
 target.PayerLiabilityOwner = TRIM(source.PayerLiabilityOwner),
 target.PriorAuthNo = TRIM(source.PriorAuthNo),
 target.DeleteFlag = source.DeleteFlag,
 target.PayerSourceChangedFlag = source.PayerSourceChangedFlag,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.SourceATableLastUpdated = source.SourceATableLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBTableLastUpdated = source.SourceBTableLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (ClaimPayerKey, ClaimKey, ClaimNumber, VisitNumber, RegionKey, Coid, SeqNumber, PayerIplanKey, PayerGroupNumber, PayerGroupName, PayerSubscriberNumber, PayerClaimIndicator, PayerLiabilityOwner, PriorAuthNo, DeleteFlag, PayerSourceChangedFlag, SourceAPrimaryKeyValue, SourceATableLastUpdated, SourceBPrimaryKeyValue, SourceBTableLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.ClaimPayerKey, source.ClaimKey, source.ClaimNumber, source.VisitNumber, source.RegionKey, TRIM(source.Coid), source.SeqNumber, source.PayerIplanKey, TRIM(source.PayerGroupNumber), TRIM(source.PayerGroupName), TRIM(source.PayerSubscriberNumber), TRIM(source.PayerClaimIndicator), TRIM(source.PayerLiabilityOwner), TRIM(source.PriorAuthNo), source.DeleteFlag, source.PayerSourceChangedFlag, source.SourceAPrimaryKeyValue, source.SourceATableLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBTableLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ClaimPayerKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimPayer
      GROUP BY ClaimPayerKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactClaimPayer');
ELSE
  COMMIT TRANSACTION;
END IF;
