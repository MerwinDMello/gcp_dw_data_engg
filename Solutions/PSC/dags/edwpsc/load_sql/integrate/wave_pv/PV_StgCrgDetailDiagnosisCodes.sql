DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgCrgDetailDiagnosisCodes AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgCrgDetailDiagnosisCodes AS source
ON 
    target.CrgHeader_DiagnosisCodePK = source.CrgHeader_DiagnosisCodePK AND 
    target.CrgDetailPK = source.CrgDetailPK AND 
    target.RegionKey = source.RegionKey
WHEN MATCHED THEN UPDATE SET
    target.CrgHeader_DiagnosisCodePK = TRIM(source.CrgHeader_DiagnosisCodePK),
    target.CrgHeader_DiagnosisCodePK_txt = TRIM(source.CrgHeader_DiagnosisCodePK_txt),
    target.CrgDetailPK = TRIM(source.CrgDetailPK),
    target.CrgDetailPK_txt = TRIM(source.CrgDetailPK_txt),
    target.CreatedOn = source.CreatedOn,
    target.CreatedBy = TRIM(source.CreatedBy),
    target.Priority = source.Priority,
    target.RegionKey = source.RegionKey,
    target.TS = source.TS,
    target.InsertedDTM = source.InsertedDTM,
    target.ModifiedDTM = source.ModifiedDTM,
    target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
    target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag,
    target.SourceSystemCode = TRIM(source.SourceSystemCode),
    target.InsertedBy = TRIM(source.InsertedBy),
    target.ModifiedBy = TRIM(source.ModifiedBy)
WHEN NOT MATCHED THEN INSERT 
(
    CrgHeader_DiagnosisCodePK, 
    CrgHeader_DiagnosisCodePK_txt, 
    CrgDetailPK, 
    CrgDetailPK_txt, 
    CreatedOn, 
    CreatedBy, 
    Priority, 
    RegionKey, 
    TS, 
    InsertedDTM, 
    ModifiedDTM, 
    DWLastUpdateDateTime, 
    SourcePhysicalDeleteFlag, 
    SourceSystemCode, 
    InsertedBy, 
    ModifiedBy
)
VALUES 
(
    TRIM(source.CrgHeader_DiagnosisCodePK), 
    TRIM(source.CrgHeader_DiagnosisCodePK_txt), 
    TRIM(source.CrgDetailPK), 
    TRIM(source.CrgDetailPK_txt), 
    source.CreatedOn, 
    TRIM(source.CreatedBy), 
    source.Priority, 
    source.RegionKey, 
    source.TS, 
    source.InsertedDTM, 
    source.ModifiedDTM, 
    source.DWLastUpdateDateTime, 
    source.SourcePhysicalDeleteFlag, 
    TRIM(source.SourceSystemCode), 
    TRIM(source.InsertedBy), 
    TRIM(source.ModifiedBy)
);

SET DUP_COUNT = (
    SELECT COUNT(*)
    FROM (
        SELECT CrgHeader_DiagnosisCodePK, CrgDetailPK, RegionKey
        FROM {{ params.param_psc_core_dataset_name }}.PV_StgCrgDetailDiagnosisCodes
        GROUP BY CrgHeader_DiagnosisCodePK, CrgDetailPK, RegionKey
        HAVING COUNT(*) > 1
    )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgLabOrder');
ELSE
  COMMIT TRANSACTION;
END IF;
