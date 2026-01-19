
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgCrgHeaderDiagnosisCodes AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgCrgHeaderDiagnosisCodes AS source
ON target.CrgHeaderDiagnosisCodePK = source.CrgHeaderDiagnosisCodePK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.CrgHeaderDiagnosisCodePK = TRIM(source.CrgHeaderDiagnosisCodePK),
 target.CrgHeaderDiagnosisCodePK_txt = TRIM(source.CrgHeaderDiagnosisCodePK_txt),
 target.CrgHeaderPK = TRIM(source.CrgHeaderPK),
 target.CrgHeaderPK_txt = TRIM(source.CrgHeaderPK_txt),
 target.ICD9Code = TRIM(source.ICD9Code),
 target.ICD10Code = TRIM(source.ICD10Code),
 target.SNOMED = TRIM(source.SNOMED),
 target.Priority = source.Priority,
 target.CreatedOn = source.CreatedOn,
 target.CreatedBy = TRIM(source.CreatedBy),
 target.ICD9Override = source.ICD9Override,
 target.RegionKey = source.RegionKey,
 target.TS = source.TS,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.ModifiedBy = TRIM(source.ModifiedBy)
WHEN NOT MATCHED THEN
  INSERT (CrgHeaderDiagnosisCodePK, CrgHeaderDiagnosisCodePK_txt, CrgHeaderPK, CrgHeaderPK_txt, ICD9Code, ICD10Code, SNOMED, Priority, CreatedOn, CreatedBy, ICD9Override, RegionKey, TS, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag, SourceSystemCode, InsertedBy, ModifiedBy)
  VALUES (TRIM(source.CrgHeaderDiagnosisCodePK), TRIM(source.CrgHeaderDiagnosisCodePK_txt), TRIM(source.CrgHeaderPK), TRIM(source.CrgHeaderPK_txt), TRIM(source.ICD9Code), TRIM(source.ICD10Code), TRIM(source.SNOMED), source.Priority, source.CreatedOn, TRIM(source.CreatedBy), source.ICD9Override, source.RegionKey, source.TS, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), TRIM(source.ModifiedBy));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CrgHeaderDiagnosisCodePK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgCrgHeaderDiagnosisCodes
      GROUP BY CrgHeaderDiagnosisCodePK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgCrgHeaderDiagnosisCodes');
ELSE
  COMMIT TRANSACTION;
END IF;
