
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefTeradataProcedureCodes AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefTeradataProcedureCodes AS source
ON target.TeradataProcedureCodeKey = source.TeradataProcedureCodeKey
WHEN MATCHED THEN
  UPDATE SET
  target.TeradataProcedureCodeKey = source.TeradataProcedureCodeKey,
 target.ProcedureCode = TRIM(source.ProcedureCode),
 target.ProcedureTypeCode = TRIM(source.ProcedureTypeCode),
 target.ProcedureCodeDescription = TRIM(source.ProcedureCodeDescription),
 target.ProcedureClassSID = TRIM(source.ProcedureClassSID),
 target.ProcedureClassDescription = TRIM(source.ProcedureClassDescription),
 target.ProcedureModalitySID = TRIM(source.ProcedureModalitySID),
 target.ProcedureModalityDescription = TRIM(source.ProcedureModalityDescription),
 target.ProcedureSpecialtySID = TRIM(source.ProcedureSpecialtySID),
 target.ProcedureSpecialtyDescription = TRIM(source.ProcedureSpecialtyDescription),
 target.EffectiveFromDateKey = source.EffectiveFromDateKey,
 target.EffectiveToDateKey = source.EffectiveToDateKey,
 target.SexEditIndicator = TRIM(source.SexEditIndicator),
 target.SourceRecordLastUpdated = source.SourceRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (TeradataProcedureCodeKey, ProcedureCode, ProcedureTypeCode, ProcedureCodeDescription, ProcedureClassSID, ProcedureClassDescription, ProcedureModalitySID, ProcedureModalityDescription, ProcedureSpecialtySID, ProcedureSpecialtyDescription, EffectiveFromDateKey, EffectiveToDateKey, SexEditIndicator, SourceRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.TeradataProcedureCodeKey, TRIM(source.ProcedureCode), TRIM(source.ProcedureTypeCode), TRIM(source.ProcedureCodeDescription), TRIM(source.ProcedureClassSID), TRIM(source.ProcedureClassDescription), TRIM(source.ProcedureModalitySID), TRIM(source.ProcedureModalityDescription), TRIM(source.ProcedureSpecialtySID), TRIM(source.ProcedureSpecialtyDescription), source.EffectiveFromDateKey, source.EffectiveToDateKey, TRIM(source.SexEditIndicator), source.SourceRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT TeradataProcedureCodeKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefTeradataProcedureCodes
      GROUP BY TeradataProcedureCodeKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefTeradataProcedureCodes');
ELSE
  COMMIT TRANSACTION;
END IF;
