
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_RefVirtualVisit AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_RefVirtualVisit AS source
ON target.VirtualVisitType = source.VirtualVisitType
WHEN MATCHED THEN
  UPDATE SET
  target.VirtualVisitType = source.VirtualVisitType,
 target.VirtualVisitDesc = TRIM(source.VirtualVisitDesc),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (VirtualVisitType, VirtualVisitDesc, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.VirtualVisitType, TRIM(source.VirtualVisitDesc), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT VirtualVisitType
      FROM {{ params.param_psc_core_dataset_name }}.PV_RefVirtualVisit
      GROUP BY VirtualVisitType
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_RefVirtualVisit');
ELSE
  COMMIT TRANSACTION;
END IF;
