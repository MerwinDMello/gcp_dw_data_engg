
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.3M_FactProFeeNotBillable AS target
USING {{ params.param_psc_stage_dataset_name }}.3M_FactProFeeNotBillable AS source
ON target.ProFeeNotBillable3MKey = source.ProFeeNotBillable3MKey
WHEN MATCHED THEN
  UPDATE SET
  target.ProFeeNotBillable3MKey = source.ProFeeNotBillable3MKey,
 target.CacAccountNumber = TRIM(source.CacAccountNumber),
 target.VisitNumber = TRIM(source.VisitNumber),
 target.VisitType = TRIM(source.VisitType),
 target.FacilityId = TRIM(source.FacilityId),
 target.LOCATION = TRIM(source.LOCATION),
 target.PatientFirstName = TRIM(source.PatientFirstName),
 target.PatientLastName = TRIM(source.PatientLastName),
 target.PatientMiddleName = TRIM(source.PatientMiddleName),
 target.AdmitDate = source.AdmitDate,
 target.DischargeDate = source.DischargeDate,
 target.ServiceDateKey = source.ServiceDateKey,
 target.ServiceDateTime = source.ServiceDateTime,
 target.CacWorklistName = TRIM(source.CacWorklistName),
 target.LookupKey = TRIM(source.LookupKey),
 target.CodingStatus = TRIM(source.CodingStatus),
 target.CodingStatusDateTime = source.CodingStatusDateTime,
 target.COMMENT = TRIM(source.COMMENT),
 target.DocNotBillableReason = TRIM(source.DocNotBillableReason),
 target.CoderId = TRIM(source.CoderId),
 target.CareProvider = TRIM(source.CareProvider),
 target.CareProviderFirstName = TRIM(source.CareProviderFirstName),
 target.CareProviderLastName = TRIM(source.CareProviderLastName),
 target.CareProviderMiddleName = TRIM(source.CareProviderMiddleName),
 target.FileName = TRIM(source.FileName),
 target.FileDate = source.FileDate,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.Coid = TRIM(source.Coid),
 target.PracticeId = TRIM(source.PracticeId),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (ProFeeNotBillable3MKey, CacAccountNumber, VisitNumber, VisitType, FacilityId, LOCATION, PatientFirstName, PatientLastName, PatientMiddleName, AdmitDate, DischargeDate, ServiceDateKey, ServiceDateTime, CacWorklistName, LookupKey, CodingStatus, CodingStatusDateTime, COMMENT, DocNotBillableReason, CoderId, CareProvider, CareProviderFirstName, CareProviderLastName, CareProviderMiddleName, FileName, FileDate, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, Coid, PracticeId, DWLastUpdateDateTime)
  VALUES (source.ProFeeNotBillable3MKey, TRIM(source.CacAccountNumber), TRIM(source.VisitNumber), TRIM(source.VisitType), TRIM(source.FacilityId), TRIM(source.LOCATION), TRIM(source.PatientFirstName), TRIM(source.PatientLastName), TRIM(source.PatientMiddleName), source.AdmitDate, source.DischargeDate, source.ServiceDateKey, source.ServiceDateTime, TRIM(source.CacWorklistName), TRIM(source.LookupKey), TRIM(source.CodingStatus), source.CodingStatusDateTime, TRIM(source.COMMENT), TRIM(source.DocNotBillableReason), TRIM(source.CoderId), TRIM(source.CareProvider), TRIM(source.CareProviderFirstName), TRIM(source.CareProviderLastName), TRIM(source.CareProviderMiddleName), TRIM(source.FileName), source.FileDate, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.Coid), TRIM(source.PracticeId), source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT ProFeeNotBillable3MKey
      FROM {{ params.param_psc_core_dataset_name }}.3M_FactProFeeNotBillable
      GROUP BY ProFeeNotBillable3MKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.3M_FactProFeeNotBillable');
ELSE
  COMMIT TRANSACTION;
END IF;
