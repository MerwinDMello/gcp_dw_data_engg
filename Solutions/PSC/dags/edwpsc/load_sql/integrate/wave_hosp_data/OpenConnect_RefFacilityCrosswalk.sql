
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.OpenConnect_RefFacilityCrosswalk AS target
USING {{ params.param_psc_stage_dataset_name }}.OpenConnect_RefFacilityCrosswalk AS source
ON target.OpenConnectFacilityCrosswalkKey = source.OpenConnectFacilityCrosswalkKey
WHEN MATCHED THEN
  UPDATE SET
  target.OpenConnectFacilityCrosswalkKey = source.OpenConnectFacilityCrosswalkKey,
 target.RegionKey = source.RegionKey,
 target.SendingApplication = TRIM(source.SendingApplication),
 target.FacilityId = TRIM(source.FacilityId),
 target.Practice = TRIM(source.Practice),
 target.VisitLocationUnit = TRIM(source.VisitLocationUnit),
 target.PatientClass = TRIM(source.PatientClass),
 target.EcwHL7Id = TRIM(source.EcwHL7Id),
 target.BeginActive = source.BeginActive,
 target.EndActive = source.EndActive,
 target.CrosswalkCreatedBy = TRIM(source.CrosswalkCreatedBy),
 target.CrosswalkCreatedDate = source.CrosswalkCreatedDate,
 target.CrosswalkModifiedBy = TRIM(source.CrosswalkModifiedBy),
 target.CrosswalkModifiedDate = source.CrosswalkModifiedDate,
 target.DeletedFlag = source.DeletedFlag,
 target.RuralHealthFlag = source.RuralHealthFlag,
 target.VERSION = source.VERSION,
 target.SourceAPrimaryKeyValue = source.SourceAPrimaryKeyValue,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.Coid = TRIM(source.Coid)
WHEN NOT MATCHED THEN
  INSERT (OpenConnectFacilityCrosswalkKey, RegionKey, SendingApplication, FacilityId, Practice, VisitLocationUnit, PatientClass, EcwHL7Id, BeginActive, EndActive, CrosswalkCreatedBy, CrosswalkCreatedDate, CrosswalkModifiedBy, CrosswalkModifiedDate, DeletedFlag, RuralHealthFlag, VERSION, SourceAPrimaryKeyValue, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, Coid)
  VALUES (source.OpenConnectFacilityCrosswalkKey, source.RegionKey, TRIM(source.SendingApplication), TRIM(source.FacilityId), TRIM(source.Practice), TRIM(source.VisitLocationUnit), TRIM(source.PatientClass), TRIM(source.EcwHL7Id), source.BeginActive, source.EndActive, TRIM(source.CrosswalkCreatedBy), source.CrosswalkCreatedDate, TRIM(source.CrosswalkModifiedBy), source.CrosswalkModifiedDate, source.DeletedFlag, source.RuralHealthFlag, source.VERSION, source.SourceAPrimaryKeyValue, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, TRIM(source.Coid));

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT OpenConnectFacilityCrosswalkKey
      FROM {{ params.param_psc_core_dataset_name }}.OpenConnect_RefFacilityCrosswalk
      GROUP BY OpenConnectFacilityCrosswalkKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.OpenConnect_RefFacilityCrosswalk');
ELSE
  COMMIT TRANSACTION;
END IF;
