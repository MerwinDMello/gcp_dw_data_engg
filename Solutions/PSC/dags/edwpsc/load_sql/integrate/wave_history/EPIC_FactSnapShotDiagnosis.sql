
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotDiagnosis AS target
USING {{ params.param_psc_stage_dataset_name }}.EPIC_FactSnapShotDiagnosis AS source
ON target.SnapShotDate = source.SnapShotDate AND target.DiagnosisKey = source.DiagnosisKey
WHEN MATCHED THEN
  UPDATE SET
  target.DiagnosisKey = source.DiagnosisKey,
 target.MonthID = source.MonthID,
 target.SnapShotDate = source.SnapShotDate,
 target.Coid = TRIM(source.Coid),
 target.RegionKey = source.RegionKey,
 target.ClaimKey = source.ClaimKey,
 target.ClaimNumber = source.ClaimNumber,
 target.VisitNumber = source.VisitNumber,
 target.GLDepartment = TRIM(source.GLDepartment),
 target.PatientID = source.PatientID,
 target.ServicingProviderKey = source.ServicingProviderKey,
 target.ServicingProviderID = TRIM(source.ServicingProviderID),
 target.RenderingProviderKey = source.RenderingProviderKey,
 target.RenderingProviderID = TRIM(source.RenderingProviderID),
 target.FacilityKey = source.FacilityKey,
 target.FacilityID = source.FacilityID,
 target.ClaimDateKey = source.ClaimDateKey,
 target.ServiceDateKey = source.ServiceDateKey,
 target.Iplan1IplanKey = source.Iplan1IplanKey,
 target.Iplan1ID = TRIM(source.Iplan1ID),
 target.FinancialClassKey = source.FinancialClassKey,
 target.DiagnosisID = source.DiagnosisID,
 target.DiagnosisCode = TRIM(source.DiagnosisCode),
 target.DiagnosisOrder = source.DiagnosisOrder,
 target.PracticeKey = source.PracticeKey,
 target.PracticeID = TRIM(source.PracticeID),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (DiagnosisKey, MonthID, SnapShotDate, Coid, RegionKey, ClaimKey, ClaimNumber, VisitNumber, GLDepartment, PatientID, ServicingProviderKey, ServicingProviderID, RenderingProviderKey, RenderingProviderID, FacilityKey, FacilityID, ClaimDateKey, ServiceDateKey, Iplan1IplanKey, Iplan1ID, FinancialClassKey, DiagnosisID, DiagnosisCode, DiagnosisOrder, PracticeKey, PracticeID, DWLastUpdateDateTime)
  VALUES (source.DiagnosisKey, source.MonthID, source.SnapShotDate, TRIM(source.Coid), source.RegionKey, source.ClaimKey, source.ClaimNumber, source.VisitNumber, TRIM(source.GLDepartment), source.PatientID, source.ServicingProviderKey, TRIM(source.ServicingProviderID), source.RenderingProviderKey, TRIM(source.RenderingProviderID), source.FacilityKey, source.FacilityID, source.ClaimDateKey, source.ServiceDateKey, source.Iplan1IplanKey, TRIM(source.Iplan1ID), source.FinancialClassKey, source.DiagnosisID, TRIM(source.DiagnosisCode), source.DiagnosisOrder, source.PracticeKey, TRIM(source.PracticeID), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate, DiagnosisKey
      FROM {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotDiagnosis
      GROUP BY SnapShotDate, DiagnosisKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.EPIC_FactSnapShotDiagnosis');
ELSE
  COMMIT TRANSACTION;
END IF;
