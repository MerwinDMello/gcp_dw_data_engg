
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactQGendaProviderHours AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactQGendaProviderHours AS source
ON target.QGendaProviderHoursKey = source.QGendaProviderHoursKey
WHEN MATCHED THEN
  UPDATE SET
  target.QGendaProviderHoursKey = source.QGendaProviderHoursKey,
 target.ProviderScheduleSK = source.ProviderScheduleSK,
 target.ScheduledCOID = TRIM(source.ScheduledCOID),
 target.CompanyCode = TRIM(source.CompanyCode),
 target.ProviderShiftTypeDesc = TRIM(source.ProviderShiftTypeDesc),
 target.ProviderStaffCategoryDesc = TRIM(source.ProviderStaffCategoryDesc),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.ProviderPersonDWId = source.ProviderPersonDWId,
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderStaffTypeDesc = TRIM(source.ProviderStaffTypeDesc),
 target.ScheduledStartDateTime = source.ScheduledStartDateTime,
 target.ScheduledEndDateTime = source.ScheduledEndDateTime,
 target.ProviderCreditedHourQty = source.ProviderCreditedHourQty,
 target.DeleteFlag = source.DeleteFlag,
 target.ProviderScheduledTypeDesc = TRIM(source.ProviderScheduledTypeDesc),
 target.SourceLastUpdatedDateTime = source.SourceLastUpdatedDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (QGendaProviderHoursKey, ProviderScheduleSK, ScheduledCOID, CompanyCode, ProviderShiftTypeDesc, ProviderStaffCategoryDesc, ProviderLastName, ProviderFirstName, ProviderPersonDWId, ProviderNPI, ProviderStaffTypeDesc, ScheduledStartDateTime, ScheduledEndDateTime, ProviderCreditedHourQty, DeleteFlag, ProviderScheduledTypeDesc, SourceLastUpdatedDateTime, SourceSystemCode, DWLastUpdateDateTime, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.QGendaProviderHoursKey, source.ProviderScheduleSK, TRIM(source.ScheduledCOID), TRIM(source.CompanyCode), TRIM(source.ProviderShiftTypeDesc), TRIM(source.ProviderStaffCategoryDesc), TRIM(source.ProviderLastName), TRIM(source.ProviderFirstName), source.ProviderPersonDWId, TRIM(source.ProviderNPI), TRIM(source.ProviderStaffTypeDesc), source.ScheduledStartDateTime, source.ScheduledEndDateTime, source.ProviderCreditedHourQty, source.DeleteFlag, TRIM(source.ProviderScheduledTypeDesc), source.SourceLastUpdatedDateTime, TRIM(source.SourceSystemCode), source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT QGendaProviderHoursKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactQGendaProviderHours
      GROUP BY QGendaProviderHoursKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactQGendaProviderHours');
ELSE
  COMMIT TRANSACTION;
END IF;
