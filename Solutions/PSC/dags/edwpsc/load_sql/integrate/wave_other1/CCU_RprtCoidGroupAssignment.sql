
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidGroupAssignment AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_RprtCoidGroupAssignment AS source
ON target.CCUCoidGroupAssignmentKey = source.CCUCoidGroupAssignmentKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUCoidGroupAssignmentKey = source.CCUCoidGroupAssignmentKey,
 target.CoderPracticeName = TRIM(source.CoderPracticeName),
 target.COID = TRIM(source.COID),
 target.AssignmentGroupValue = TRIM(source.AssignmentGroupValue),
 target.COIDSpecialty = TRIM(source.COIDSpecialty),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (CCUCoidGroupAssignmentKey, CoderPracticeName, COID, AssignmentGroupValue, COIDSpecialty, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.CCUCoidGroupAssignmentKey, TRIM(source.CoderPracticeName), TRIM(source.COID), TRIM(source.AssignmentGroupValue), TRIM(source.COIDSpecialty), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUCoidGroupAssignmentKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidGroupAssignment
      GROUP BY CCUCoidGroupAssignmentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_RprtCoidGroupAssignment');
ELSE
  COMMIT TRANSACTION;
END IF;
