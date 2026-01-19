
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgLabOrder AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgLabOrder AS source
ON target.LabOrderPK = source.LabOrderPK AND target.regionkey = source.regionkey AND target.LabOrderID = source.LabOrderID
WHEN MATCHED THEN
  UPDATE SET
  target.PracticePK = TRIM(source.PracticePK),
 target.LocationID = source.LocationID,
 target.LabOrderID = source.LabOrderID,
 target.PatientID = source.PatientID,
 target.PatientName = TRIM(source.PatientName),
 target.PatientDOB = source.PatientDOB,
 target.Gender = TRIM(source.Gender),
 target.EthnicityID = source.EthnicityID,
 target.RequestedBy = source.RequestedBy,
 target.RequestedByName = TRIM(source.RequestedByName),
 target.Priority = source.Priority,
 target.DateRequested = source.DateRequested,
 target.PlacerOrderNumber = TRIM(source.PlacerOrderNumber),
 target.RefLabOrderNumber = TRIM(source.RefLabOrderNumber),
 target.RefLabAccessionNumber = TRIM(source.RefLabAccessionNumber),
 target.SpecimenDrawn = source.SpecimenDrawn,
 target.LabOrderStatus = source.LabOrderStatus,
 target.EnteredBy = TRIM(source.EnteredBy),
 target.ChangedBy = TRIM(source.ChangedBy),
 target.DateIn = source.DateIn,
 target.DateChange = source.DateChange,
 target.ChartPK = TRIM(source.ChartPK),
 target.LabFacilityType = source.LabFacilityType,
 target.NotificationUserProfilePK = TRIM(source.NotificationUserProfilePK),
 target.DateSent = source.DateSent,
 target.WhoPaysLab = source.WhoPaysLab,
 target.PSCHoldFlag = source.PSCHoldFlag,
 target.ShowReconciledMessage = source.ShowReconciledMessage,
 target.PlacerGroupNumber = TRIM(source.PlacerGroupNumber),
 target.PatNameSpace = TRIM(source.PatNameSpace),
 target.PlacerNameSpace = TRIM(source.PlacerNameSpace),
 target.FillerNameSpace = TRIM(source.FillerNameSpace),
 target.PatTypeCode = TRIM(source.PatTypeCode),
 target.SpecimenActionCode = TRIM(source.SpecimenActionCode),
 target.OrderingProviderName = TRIM(source.OrderingProviderName),
 target.OrderingProviderID = TRIM(source.OrderingProviderID),
 target.OrderingProviderNameTypeCode = TRIM(source.OrderingProviderNameTypeCode),
 target.OrderingProviderIDTypeCode = TRIM(source.OrderingProviderIDTypeCode),
 target.ABNResponseCode = source.ABNResponseCode,
 target.FacilityCode = TRIM(source.FacilityCode),
 target.InterfaceType = source.InterfaceType,
 target.LabFacilityID = source.LabFacilityID,
 target.LabOrderPK = TRIM(source.LabOrderPK),
 target.LabProcessingStatus = source.LabProcessingStatus,
 target.RegionKey = source.RegionKey,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (PracticePK, LocationID, LabOrderID, PatientID, PatientName, PatientDOB, Gender, EthnicityID, RequestedBy, RequestedByName, Priority, DateRequested, PlacerOrderNumber, RefLabOrderNumber, RefLabAccessionNumber, SpecimenDrawn, LabOrderStatus, EnteredBy, ChangedBy, DateIn, DateChange, ChartPK, LabFacilityType, NotificationUserProfilePK, DateSent, WhoPaysLab, PSCHoldFlag, ShowReconciledMessage, PlacerGroupNumber, PatNameSpace, PlacerNameSpace, FillerNameSpace, PatTypeCode, SpecimenActionCode, OrderingProviderName, OrderingProviderID, OrderingProviderNameTypeCode, OrderingProviderIDTypeCode, ABNResponseCode, FacilityCode, InterfaceType, LabFacilityID, LabOrderPK, LabProcessingStatus, RegionKey, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.PracticePK), source.LocationID, source.LabOrderID, source.PatientID, TRIM(source.PatientName), source.PatientDOB, TRIM(source.Gender), source.EthnicityID, source.RequestedBy, TRIM(source.RequestedByName), source.Priority, source.DateRequested, TRIM(source.PlacerOrderNumber), TRIM(source.RefLabOrderNumber), TRIM(source.RefLabAccessionNumber), source.SpecimenDrawn, source.LabOrderStatus, TRIM(source.EnteredBy), TRIM(source.ChangedBy), source.DateIn, source.DateChange, TRIM(source.ChartPK), source.LabFacilityType, TRIM(source.NotificationUserProfilePK), source.DateSent, source.WhoPaysLab, source.PSCHoldFlag, source.ShowReconciledMessage, TRIM(source.PlacerGroupNumber), TRIM(source.PatNameSpace), TRIM(source.PlacerNameSpace), TRIM(source.FillerNameSpace), TRIM(source.PatTypeCode), TRIM(source.SpecimenActionCode), TRIM(source.OrderingProviderName), TRIM(source.OrderingProviderID), TRIM(source.OrderingProviderNameTypeCode), TRIM(source.OrderingProviderIDTypeCode), source.ABNResponseCode, TRIM(source.FacilityCode), source.InterfaceType, source.LabFacilityID, TRIM(source.LabOrderPK), source.LabProcessingStatus, source.RegionKey, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT LabOrderPK, regionkey, LabOrderID
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgLabOrder
      GROUP BY LabOrderPK, regionkey, LabOrderID
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgLabOrder');
ELSE
  COMMIT TRANSACTION;
END IF;
