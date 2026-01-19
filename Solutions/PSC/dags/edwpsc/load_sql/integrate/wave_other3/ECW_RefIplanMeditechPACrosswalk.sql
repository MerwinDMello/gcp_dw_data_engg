
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechPACrosswalk AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefIplanMeditechPACrosswalk AS source
ON target.MeditechPACrosswalkKey = source.MeditechPACrosswalkKey
WHEN MATCHED THEN
  UPDATE SET
  target.MeditechPACrosswalkKey = source.MeditechPACrosswalkKey,
 target.SSCState = TRIM(source.SSCState),
 target.SSCName = TRIM(source.SSCName),
 target.HospitalCOID = TRIM(source.HospitalCOID),
 target.MeditechIplanID = TRIM(source.MeditechIplanID),
 target.PAIplanID = source.PAIplanID,
 target.PAIplanName = TRIM(source.PAIplanName),
 target.PAIplanFinancialClassKey = source.PAIplanFinancialClassKey,
 target.PAInsuranceActiveFlag = TRIM(source.PAInsuranceActiveFlag),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (MeditechPACrosswalkKey, SSCState, SSCName, HospitalCOID, MeditechIplanID, PAIplanID, PAIplanName, PAIplanFinancialClassKey, PAInsuranceActiveFlag, DWLastUpdateDateTime)
  VALUES (source.MeditechPACrosswalkKey, TRIM(source.SSCState), TRIM(source.SSCName), TRIM(source.HospitalCOID), TRIM(source.MeditechIplanID), source.PAIplanID, TRIM(source.PAIplanName), source.PAIplanFinancialClassKey, TRIM(source.PAInsuranceActiveFlag), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT MeditechPACrosswalkKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechPACrosswalk
      GROUP BY MeditechPACrosswalkKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefIplanMeditechPACrosswalk');
ELSE
  COMMIT TRANSACTION;
END IF;
