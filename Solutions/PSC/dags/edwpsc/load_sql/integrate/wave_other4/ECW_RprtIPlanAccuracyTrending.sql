
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtIPlanAccuracyTrending AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtIPlanAccuracyTrending AS source
ON target.IPlanAccuracyTrendKey = source.IPlanAccuracyTrendKey
WHEN MATCHED THEN
  UPDATE SET
  target.IPlanAccuracyTrendKey = source.IPlanAccuracyTrendKey,
 target.SnapShotDate = source.SnapShotDate,
 target.DOSYear = source.DOSYear,
 target.DOSMonth = TRIM(source.DOSMonth),
 target.ServiceDateKey = source.ServiceDateKey,
 target.PracticeID = source.PracticeID,
 target.Coid = TRIM(source.Coid),
 target.FacilityIdType = source.FacilityIdType,
 target.POSName = TRIM(source.POSName),
 target.POSCategoryName = TRIM(source.POSCategoryName),
 target.ClaimNumber = source.ClaimNumber,
 target.OriginalPayer1Iplan = TRIM(source.OriginalPayer1Iplan),
 target.OriginalPayer1FinClass = source.OriginalPayer1FinClass,
 target.OriginalPayer1ID = TRIM(source.OriginalPayer1ID),
 target.ChargesBucket = TRIM(source.ChargesBucket),
 target.OriginalCharges = source.OriginalCharges,
 target.VoidFlag = source.VoidFlag,
 target.DaysToChange = source.DaysToChange,
 target.NewClaimNumber = source.NewClaimNumber,
 target.ChangeDate = source.ChangeDate,
 target.NewPayer1Iplan = TRIM(source.NewPayer1Iplan),
 target.NewPayer1FinClass = source.NewPayer1FinClass,
 target.NewPayer1ID = TRIM(source.NewPayer1ID),
 target.Claims = source.Claims,
 target.OriginalPayerPendingFlag = TRIM(source.OriginalPayerPendingFlag),
 target.ConversionsToFlag = TRIM(source.ConversionsToFlag),
 target.QualifyType = TRIM(source.QualifyType),
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.OriginalClaimKey = source.OriginalClaimKey,
 target.OriginalPayer1IplanKey = source.OriginalPayer1IplanKey,
 target.NewClaimKey = source.NewClaimKey,
 target.NewPayer1IplanKey = source.NewPayer1IplanKey
WHEN NOT MATCHED THEN
  INSERT (IPlanAccuracyTrendKey, SnapShotDate, DOSYear, DOSMonth, ServiceDateKey, PracticeID, Coid, FacilityIdType, POSName, POSCategoryName, ClaimNumber, OriginalPayer1Iplan, OriginalPayer1FinClass, OriginalPayer1ID, ChargesBucket, OriginalCharges, VoidFlag, DaysToChange, NewClaimNumber, ChangeDate, NewPayer1Iplan, NewPayer1FinClass, NewPayer1ID, Claims, OriginalPayerPendingFlag, ConversionsToFlag, QualifyType, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, OriginalClaimKey, OriginalPayer1IplanKey, NewClaimKey, NewPayer1IplanKey)
  VALUES (source.IPlanAccuracyTrendKey, source.SnapShotDate, source.DOSYear, TRIM(source.DOSMonth), source.ServiceDateKey, source.PracticeID, TRIM(source.Coid), source.FacilityIdType, TRIM(source.POSName), TRIM(source.POSCategoryName), source.ClaimNumber, TRIM(source.OriginalPayer1Iplan), source.OriginalPayer1FinClass, TRIM(source.OriginalPayer1ID), TRIM(source.ChargesBucket), source.OriginalCharges, source.VoidFlag, source.DaysToChange, source.NewClaimNumber, source.ChangeDate, TRIM(source.NewPayer1Iplan), source.NewPayer1FinClass, TRIM(source.NewPayer1ID), source.Claims, TRIM(source.OriginalPayerPendingFlag), TRIM(source.ConversionsToFlag), TRIM(source.QualifyType), TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.OriginalClaimKey, source.OriginalPayer1IplanKey, source.NewClaimKey, source.NewPayer1IplanKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT IPlanAccuracyTrendKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtIPlanAccuracyTrending
      GROUP BY IPlanAccuracyTrendKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtIPlanAccuracyTrending');
ELSE
  COMMIT TRANSACTION;
END IF;
