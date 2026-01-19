
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtICCClaimTimeline AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtICCClaimTimeline AS source
ON target.SnapShotDate = source.SnapShotDate AND target.ClaimKey = source.ClaimKey
WHEN MATCHED THEN
  UPDATE SET
  target.SnapshotDate = source.SnapshotDate,
 target.ClaimKey = source.ClaimKey,
 target.Coid = TRIM(source.Coid),
 target.ServicingProviderName = TRIM(source.ServicingProviderName),
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.PrimaryIplanName = TRIM(source.PrimaryIplanName),
 target.PrimaryInsFinancialClass = TRIM(source.PrimaryInsFinancialClass),
 target.ServiceDateKey = source.ServiceDateKey,
 target.ClaimDateKey = source.ClaimDateKey,
 target.FirstPatientStatementBillDateKey = source.FirstPatientStatementBillDateKey,
 target.FirstPatientClaimPaymentDateKey = source.FirstPatientClaimPaymentDateKey,
 target.DaysFromServiceToClaimKeydate = source.DaysFromServiceToClaimKeydate,
 target.RHPrimaryMinSubmissionDateKey = source.RHPrimaryMinSubmissionDateKey,
 target.DaysFromClaimKeyToSubmitDate = source.DaysFromClaimKeyToSubmitDate,
 target.RHPrimaryMinReleaseDateKey = source.RHPrimaryMinReleaseDateKey,
 target.RHPrimaryReleaseCount = source.RHPrimaryReleaseCount,
 target.DaysFromSubmitToReleaseDate = source.DaysFromSubmitToReleaseDate,
 target.RelayReleaseStatus = TRIM(source.RelayReleaseStatus),
 target.PayerFirstClaimPaymentDateKey = source.PayerFirstClaimPaymentDateKey,
 target.DaysFromReleaseToFirstPaymentDate = source.DaysFromReleaseToFirstPaymentDate,
 target.PayerLastClaimPaymentDateKey = source.PayerLastClaimPaymentDateKey,
 target.DaysFromReleaseToLastPaymentDate = source.DaysFromReleaseToLastPaymentDate,
 target.DaysFromFirstToLastPaymentDate = source.DaysFromFirstToLastPaymentDate,
 target.TotalChargesAmt = source.TotalChargesAmt,
 target.TotalPrimaryInsurancePayments = source.TotalPrimaryInsurancePayments,
 target.FirstPrimaryInsurancePayment = source.FirstPrimaryInsurancePayment,
 target.LastMinusFirstPayment = source.LastMinusFirstPayment,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (SnapshotDate, ClaimKey, Coid, ServicingProviderName, Iplan1IplanKey, PrimaryIplanName, PrimaryInsFinancialClass, ServiceDateKey, ClaimDateKey, FirstPatientStatementBillDateKey, FirstPatientClaimPaymentDateKey, DaysFromServiceToClaimKeydate, RHPrimaryMinSubmissionDateKey, DaysFromClaimKeyToSubmitDate, RHPrimaryMinReleaseDateKey, RHPrimaryReleaseCount, DaysFromSubmitToReleaseDate, RelayReleaseStatus, PayerFirstClaimPaymentDateKey, DaysFromReleaseToFirstPaymentDate, PayerLastClaimPaymentDateKey, DaysFromReleaseToLastPaymentDate, DaysFromFirstToLastPaymentDate, TotalChargesAmt, TotalPrimaryInsurancePayments, FirstPrimaryInsurancePayment, LastMinusFirstPayment, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, DWLastUpdateDateTime)
  VALUES (source.SnapshotDate, source.ClaimKey, TRIM(source.Coid), TRIM(source.ServicingProviderName), source.Iplan1IplanKey, TRIM(source.PrimaryIplanName), TRIM(source.PrimaryInsFinancialClass), source.ServiceDateKey, source.ClaimDateKey, source.FirstPatientStatementBillDateKey, source.FirstPatientClaimPaymentDateKey, source.DaysFromServiceToClaimKeydate, source.RHPrimaryMinSubmissionDateKey, source.DaysFromClaimKeyToSubmitDate, source.RHPrimaryMinReleaseDateKey, source.RHPrimaryReleaseCount, source.DaysFromSubmitToReleaseDate, TRIM(source.RelayReleaseStatus), source.PayerFirstClaimPaymentDateKey, source.DaysFromReleaseToFirstPaymentDate, source.PayerLastClaimPaymentDateKey, source.DaysFromReleaseToLastPaymentDate, source.DaysFromFirstToLastPaymentDate, source.TotalChargesAmt, source.TotalPrimaryInsurancePayments, source.FirstPrimaryInsurancePayment, source.LastMinusFirstPayment, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, ClaimKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtICCClaimTimeline
      GROUP BY SnapShotDate, ClaimKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtICCClaimTimeline');
ELSE
  COMMIT TRANSACTION;
END IF;
