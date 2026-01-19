
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotDiagnosis AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactSnapShotDiagnosis AS source
ON target.snapshotdate = source.snapshotdate AND target.diagnosiskey = source.diagnosiskey
WHEN MATCHED THEN
  UPDATE SET
  target.DiagnosisKey = source.DiagnosisKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.GLDepartment = TRIM(source.GLDepartment),
 target.PatientID = source.PatientID,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = source.ServicingProviderID,
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = source.RenderingProviderID,
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = source.Iplan1ID,
 target.FinancialClassKey = source.FinancialClassKey,
 target.DiagnosisID = source.DiagnosisID,
 target.DiagnosisCode = TRIM(source.DiagnosisCode),
 target.DiagnosisOrder = source.DiagnosisOrder,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (DiagnosisKey, MonthID, SnapShotDate, Coid, RegionKey, ClaimKey, ClaimNumber, GLDepartment, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ServiceDateKey, Iplan1IplanKey, Iplan1ID, FinancialClassKey, DiagnosisID, DiagnosisCode, DiagnosisOrder, DWLastUpdateDateTime)
  VALUES (source.DiagnosisKey, source.MonthID, source.SnapShotDate, TRIM(source.Coid), source.RegionKey, source.ClaimKey, source.ClaimNumber, TRIM(source.GLDepartment), source.PatientID, source.ServicingProviderKey, source.ServicingProviderID, source.RenderingProviderKey, source.RenderingProviderID, source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ServiceDateKey, source.Iplan1IplanKey, source.Iplan1ID, source.FinancialClassKey, source.DiagnosisID, TRIM(source.DiagnosisCode), source.DiagnosisOrder, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT snapshotdate, diagnosiskey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotDiagnosis
      GROUP BY snapshotdate, diagnosiskey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactSnapShotDiagnosis');
ELSE
  COMMIT TRANSACTION;
END IF;
