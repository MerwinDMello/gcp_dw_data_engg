
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RefPatientGuarantorMeditechExpanse AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RefPatientGuarantorMeditechExpanse AS source
ON target.PatientGuarantorKey = source.PatientGuarantorKey
WHEN MATCHED THEN
  UPDATE SET
  target.PatientGuarantorKey = source.PatientGuarantorKey,
 target.RegionKey = source.RegionKey,
 target.GuarantorNumber = TRIM(source.GuarantorNumber),
 target.RelationshipCode = TRIM(source.RelationshipCode),
 target.RelationshipName = TRIM(source.RelationshipName),
 target.GuarantorLastName = TRIM(source.GuarantorLastName),
 target.GuarantorNameFirst = TRIM(source.GuarantorNameFirst),
 target.GuarantorNameMiddle = TRIM(source.GuarantorNameMiddle),
 target.GuarantorAddressLine1 = TRIM(source.GuarantorAddressLine1),
 target.GuarantorAddressLine2 = TRIM(source.GuarantorAddressLine2),
 target.GuarantorGeographyKey = source.GuarantorGeographyKey,
 target.SourceAPrimaryKeyValue = TRIM(source.SourceAPrimaryKeyValue),
 target.SourceARecordLastUpdated = source.SourceARecordLastUpdated,
 target.SourceBPrimaryKeyValue = TRIM(source.SourceBPrimaryKeyValue),
 target.SourceBRecordLastUpdated = source.SourceBRecordLastUpdated,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (PatientGuarantorKey, RegionKey, GuarantorNumber, RelationshipCode, RelationshipName, GuarantorLastName, GuarantorNameFirst, GuarantorNameMiddle, GuarantorAddressLine1, GuarantorAddressLine2, GuarantorGeographyKey, SourceAPrimaryKeyValue, SourceARecordLastUpdated, SourceBPrimaryKeyValue, SourceBRecordLastUpdated, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.PatientGuarantorKey, source.RegionKey, TRIM(source.GuarantorNumber), TRIM(source.RelationshipCode), TRIM(source.RelationshipName), TRIM(source.GuarantorLastName), TRIM(source.GuarantorNameFirst), TRIM(source.GuarantorNameMiddle), TRIM(source.GuarantorAddressLine1), TRIM(source.GuarantorAddressLine2), source.GuarantorGeographyKey, TRIM(source.SourceAPrimaryKeyValue), source.SourceARecordLastUpdated, TRIM(source.SourceBPrimaryKeyValue), source.SourceBRecordLastUpdated, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatientGuarantorKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RefPatientGuarantorMeditechExpanse
      GROUP BY PatientGuarantorKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RefPatientGuarantorMeditechExpanse');
ELSE
  COMMIT TRANSACTION;
END IF;
