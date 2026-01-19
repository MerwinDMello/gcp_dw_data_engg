
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotEncountersWithoutClaimsWithPayments AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotEncountersWithoutClaimsWithPayments AS source
ON target.SnapShotDate = source.SnapShotDate AND target.EncountersWithoutClaimsWIthPaymentsKey = source.EncountersWithoutClaimsWIthPaymentsKey
WHEN MATCHED THEN
  UPDATE SET
  target.EncountersWithoutClaimsWithPaymentsKey = source.EncountersWithoutClaimsWithPaymentsKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.FinacialClassKey = source.FinacialClassKey,
 target.FacilityID = source.FacilityID,
 target.FacilityKey = source.FacilityKey,
 target.EncounterID = source.EncounterID,
 target.EncounterKey = source.EncounterKey,
 target.DosProviderID = TRIM(source.DosProviderID),
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.TotalPaymentAmt = source.TotalPaymentAmt,
 target.TotalPatientPaymentAmt = source.TotalPatientPaymentAmt,
 target.LastPaymentDateKey = source.LastPaymentDateKey,
 target.LastPatientPaymentDateKey = source.LastPatientPaymentDateKey,
 target.ServiceDate = source.ServiceDate,
 target.PatientID = source.PatientID,
 target.PatientKey = source.PatientKey,
 target.TotalBalanceAmt = source.TotalBalanceAmt,
 target.PracticeKey = source.PracticeKey,
 target.RegionKey = source.RegionKey
WHEN NOT MATCHED THEN
  INSERT (EncountersWithoutClaimsWithPaymentsKey, MonthID, SnapShotDate, Coid, FinacialClassKey, FacilityID, FacilityKey, EncounterID, EncounterKey, DosProviderID, ServicingProviderKey, TotalPaymentAmt, TotalPatientPaymentAmt, LastPaymentDateKey, LastPatientPaymentDateKey, ServiceDate, PatientID, PatientKey, TotalBalanceAmt, PracticeKey, RegionKey)
  VALUES (source.EncountersWithoutClaimsWithPaymentsKey, source.MonthID, source.SnapShotDate, TRIM(source.Coid), source.FinacialClassKey, source.FacilityID, source.FacilityKey, source.EncounterID, source.EncounterKey, TRIM(source.DosProviderID), source.ServicingProviderKey, source.TotalPaymentAmt, source.TotalPatientPaymentAmt, source.LastPaymentDateKey, source.LastPatientPaymentDateKey, source.ServiceDate, source.PatientID, source.PatientKey, source.TotalBalanceAmt, source.PracticeKey, source.RegionKey);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, EncountersWithoutClaimsWIthPaymentsKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotEncountersWithoutClaimsWithPayments
      GROUP BY SnapShotDate, EncountersWithoutClaimsWIthPaymentsKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotEncountersWithoutClaimsWithPayments');
ELSE
  COMMIT TRANSACTION;
END IF;
