
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactQGendaActualHours AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactQGendaActualHours AS source
ON target.QGendaProviderActualHoursKey = source.QGendaProviderActualHoursKey
WHEN MATCHED THEN
  UPDATE SET
  target.QGendaProviderActualHoursKey = source.QGendaProviderActualHoursKey,
 target.ProviderActualHourSK = source.ProviderActualHourSK,
 target.Coid = TRIM(source.Coid),
 target.CompanyCode = TRIM(source.CompanyCode),
 target.LocationSK = source.LocationSK,
 target.ProviderScheduledTypeDesc = TRIM(source.ProviderScheduledTypeDesc),
 target.ProviderShiftTypeDesc = TRIM(source.ProviderShiftTypeDesc),
 target.ProviderStaffCategoryDesc = TRIM(source.ProviderStaffCategoryDesc),
 target.LastName = TRIM(source.LastName),
 target.FirstName = TRIM(source.FirstName),
 target.NPI = TRIM(source.NPI),
 target.ProviderStaffTypeDesc = TRIM(source.ProviderStaffTypeDesc),
 target.ProviderActualStartDatetime = source.ProviderActualStartDatetime,
 target.ProviderActualEndDatetime = source.ProviderActualEndDatetime,
 target.ProviderActualMinuteQty = source.ProviderActualMinuteQty,
 target.ProviderEffectiveStartDatetime = source.ProviderEffectiveStartDatetime,
 target.ProviderEffectiveEndDatetime = source.ProviderEffectiveEndDatetime,
 target.ProviderEffectiveMinuteQty = source.ProviderEffectiveMinuteQty,
 target.DeletedFlag = source.DeletedFlag,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDatetime = source.DWLastUpdateDatetime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (QGendaProviderActualHoursKey, ProviderActualHourSK, Coid, CompanyCode, LocationSK, ProviderScheduledTypeDesc, ProviderShiftTypeDesc, ProviderStaffCategoryDesc, LastName, FirstName, NPI, ProviderStaffTypeDesc, ProviderActualStartDatetime, ProviderActualEndDatetime, ProviderActualMinuteQty, ProviderEffectiveStartDatetime, ProviderEffectiveEndDatetime, ProviderEffectiveMinuteQty, DeletedFlag, SourceSystemCode, DWLastUpdateDatetime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.QGendaProviderActualHoursKey, source.ProviderActualHourSK, TRIM(source.Coid), TRIM(source.CompanyCode), source.LocationSK, TRIM(source.ProviderScheduledTypeDesc), TRIM(source.ProviderShiftTypeDesc), TRIM(source.ProviderStaffCategoryDesc), TRIM(source.LastName), TRIM(source.FirstName), TRIM(source.NPI), TRIM(source.ProviderStaffTypeDesc), source.ProviderActualStartDatetime, source.ProviderActualEndDatetime, source.ProviderActualMinuteQty, source.ProviderEffectiveStartDatetime, source.ProviderEffectiveEndDatetime, source.ProviderEffectiveMinuteQty, source.DeletedFlag, TRIM(source.SourceSystemCode), source.DWLastUpdateDatetime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT QGendaProviderActualHoursKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactQGendaActualHours
      GROUP BY QGendaProviderActualHoursKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactQGendaActualHours');
ELSE
  COMMIT TRANSACTION;
END IF;
