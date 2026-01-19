
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotEncountersWithoutClaimsWithPayments AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_FactSnapShotEncountersWithoutClaimsWithPayments AS source
ON target.SnapShotDate = source.SnapShotDate AND target.EncountersWithoutClaimsWIthPaymentsKey = source.EncountersWithoutClaimsWIthPaymentsKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncountersWithoutClaimsWithPaymentsKey = source.EncountersWithoutClaimsWithPaymentsKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.coid = TRIM(source.coid),
 target.FinacialClassKey = source.FinacialClassKey,
 target.FacilityID = source.FacilityID,
 target.FacilityKey = source.FacilityKey,
 target.EncounterID = source.EncounterID,
 target.EncounterKey = source.EncounterKey,
 target.DosProviderID = source.DosProviderID,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.TotalPaymentAmt = source.TotalPaymentAmt,
 target.TotalPatientPaymentAmt = source.TotalPatientPaymentAmt,
 target.LastPaymentDateKey = source.LastPaymentDateKey,
 target.LastPatientPaymentDateKey = source.LastPatientPaymentDateKey,
 target.ServiceDate = source.ServiceDate,
 target.PatientID = source.PatientID,
 target.PatientKey = source.PatientKey,
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.PracticeID = TRIM(source.PracticeID),
 target.PracticeKey = source.PracticeKey,
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (EncountersWithoutClaimsWithPaymentsKey, MonthID, SnapShotDate, coid, FinacialClassKey, FacilityID, FacilityKey, EncounterID, EncounterKey, DosProviderID, ServicingProviderKey, TotalPaymentAmt, TotalPatientPaymentAmt, LastPaymentDateKey, LastPatientPaymentDateKey, ServiceDate, PatientID, PatientKey, TotalBalanceAmt, PracticeID, PracticeKey, RegionKey)
  VALUES (source.EncountersWithoutClaimsWithPaymentsKey, source.MonthID, source.SnapShotDate, TRIM(source.coid), source.FinacialClassKey, source.FacilityID, source.FacilityKey, source.EncounterID, source.EncounterKey, source.DosProviderID, source.ServicingProviderKey, source.TotalPaymentAmt, source.TotalPatientPaymentAmt, source.LastPaymentDateKey, source.LastPatientPaymentDateKey, source.ServiceDate, source.PatientID, source.PatientKey, source.TotalBalanceAmt, TRIM(source.PracticeID), source.PracticeKey, source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, EncountersWithoutClaimsWIthPaymentsKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotEncountersWithoutClaimsWithPayments
      GROUP BY SnapShotDate, EncountersWithoutClaimsWIthPaymentsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_FactSnapShotEncountersWithoutClaimsWithPayments');
ELSE
  COMMIT TRANSACTION;
END IF;
