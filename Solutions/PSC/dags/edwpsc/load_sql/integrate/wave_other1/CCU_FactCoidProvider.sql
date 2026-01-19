
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.CCU_FactCoidProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.CCU_FactCoidProvider AS source
ON target.CCUCoidProviderKey = source.CCUCoidProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.CCUCoidProviderKey = source.CCUCoidProviderKey,
 target.Coid = TRIM(source.Coid),
 target.CoidName = TRIM(source.CoidName),
 target.CoidNameOnly = TRIM(source.CoidNameOnly),
 target.GLDepartmentNumber = TRIM(source.GLDepartmentNumber),
 target.LookupValue = TRIM(source.LookupValue),
 target.CenterDescription = TRIM(source.CenterDescription),
 target.GroupName = TRIM(source.GroupName),
 target.DivisionName = TRIM(source.DivisionName),
 target.MarketName = TRIM(source.MarketName),
 target.CoidStartDateKey = source.CoidStartDateKey,
 target.CoidTermDateKey = source.CoidTermDateKey,
 target.CoidLOB = TRIM(source.CoidLOB),
 target.CoidSublob = TRIM(source.CoidSublob),
 target.CoidSpecialty = TRIM(source.CoidSpecialty),
 target.GmeCoid = TRIM(source.GmeCoid),
 target.EcwCoid = TRIM(source.EcwCoid),
 target.PKCoid = TRIM(source.PKCoid),
 target.EpicCoid = TRIM(source.EpicCoid),
 target.PVCoid = TRIM(source.PVCoid),
 target.CoidSystem = TRIM(source.CoidSystem),
 target.PPMSFlag = source.PPMSFlag,
 target.CoidConsolidationDate = TRIM(source.CoidConsolidationDate),
 target.CCUDiscontinuedDate = TRIM(source.CCUDiscontinuedDate),
 target.PKActivationStatus = TRIM(source.PKActivationStatus),
 target.PKEnvironment = TRIM(source.PKEnvironment),
 target.CoidCount = source.CoidCount,
 target.ActivityCoid = TRIM(source.ActivityCoid),
 target.ArClaimCountCoid = source.ArClaimCountCoid,
 target.EwocCountCoid = source.EwocCountCoid,
 target.CoidCentralizedStatus = TRIM(source.CoidCentralizedStatus),
 target.CoidStatus = TRIM(source.CoidStatus),
 target.CoidLevelOfCentralization = TRIM(source.CoidLevelOfCentralization),
 target.PartialCentralizationReason = TRIM(source.PartialCentralizationReason),
 target.CoidOptsOutCCU = TRIM(source.CoidOptsOutCCU),
 target.CoidPOSVariation = TRIM(source.CoidPOSVariation),
 target.CoidPOSVariationLabel = TRIM(source.CoidPOSVariationLabel),
 target.CoidTOSVariation = TRIM(source.CoidTOSVariation),
 target.CoidTOSVariationLabel = TRIM(source.CoidTOSVariationLabel),
 target.CoidWorkFlow = TRIM(source.CoidWorkFlow),
 target.CoidWorkFlowCount = source.CoidWorkFlowCount,
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderName = TRIM(source.ProviderName),
 target.SpecialtyName = TRIM(source.SpecialtyName),
 target.ProviderStartDateKey = source.ProviderStartDateKey,
 target.ProviderTermDateKey = source.ProviderTermDateKey,
 target.ProviderFutureTermDateKey = source.ProviderFutureTermDateKey,
 target.ProviderStatus = TRIM(source.ProviderStatus),
 target.ProviderCentralizedStatus = TRIM(source.ProviderCentralizedStatus),
 target.ProviderOptsOutCCU = TRIM(source.ProviderOptsOutCCU),
 target.ProviderOptsOutCCUCoid = TRIM(source.ProviderOptsOutCCUCoid),
 target.ProviderGroupAssignment = TRIM(source.ProviderGroupAssignment),
 target.ProviderPOSVariation = TRIM(source.ProviderPOSVariation),
 target.ProviderPOSVariationLabel = TRIM(source.ProviderPOSVariationLabel),
 target.ProviderTOSVariation = TRIM(source.ProviderTOSVariation),
 target.ProviderTOSVariationLabel = TRIM(source.ProviderTOSVariationLabel),
 target.ProviderCoidPOSVariation = TRIM(source.ProviderCoidPOSVariation),
 target.ProviderCoidPOSVariationLabel = TRIM(source.ProviderCoidPOSVariationLabel),
 target.ProviderCoidTOSVariation = TRIM(source.ProviderCoidTOSVariation),
 target.ProviderCoidTOSVariationLabel = TRIM(source.ProviderCoidTOSVariationLabel),
 target.ProviderWorkFlow = TRIM(source.ProviderWorkFlow),
 target.ProviderWorkFlowCount = source.ProviderWorkFlowCount,
 target.ProviderActivity = TRIM(source.ProviderActivity),
 target.ProviderArClaimCount = source.ProviderArClaimCount,
 target.ProviderEwocEncounterCount = source.ProviderEwocEncounterCount,
 target.MultipleCoidAssociation = TRIM(source.MultipleCoidAssociation),
 target.ProviderCountActive = source.ProviderCountActive,
 target.ProviderCountTermed = source.ProviderCountTermed,
 target.ProviderCountArchiveClosed = source.ProviderCountArchiveClosed,
 target.ProviderCountNA = source.ProviderCountNA,
 target.FTE = source.FTE,
 target.CoidExclusionFlag = TRIM(source.CoidExclusionFlag),
 target.MORSnapShotDateKey = source.MORSnapShotDateKey,
 target.PaperProcess = TRIM(source.PaperProcess),
 target.PaperProcessDetail = TRIM(source.PaperProcessDetail),
 target.EmbeddedCoder = TRIM(source.EmbeddedCoder),
 target.DedicatedCoder = TRIM(source.DedicatedCoder),
 target.MissingDocVariation = TRIM(source.MissingDocVariation),
 target.MissingDocProcessVariation = TRIM(source.MissingDocProcessVariation),
 target.MissingDocCategorySharepointArtiva = TRIM(source.MissingDocCategorySharepointArtiva),
 target.CodeChangeVariation = TRIM(source.CodeChangeVariation),
 target.CodeChangeVariationDetail = TRIM(source.CodeChangeVariationDetail),
 target.ChargeOverride = TRIM(source.ChargeOverride),
 target.SelfCoding = TRIM(source.SelfCoding),
 target.`100PercentAudit` = TRIM(source.`100PercentAudit`),
 target.CCUPerformsChargeEntry = TRIM(source.CCUPerformsChargeEntry),
 target.CCUAssignsProviderTVU = TRIM(source.CCUAssignsProviderTVU),
 target.NotUsingECWResourceSchedule = TRIM(source.NotUsingECWResourceSchedule),
 target.NonHCAFacilities = TRIM(source.NonHCAFacilities),
 target.ProviderUsingPKChargeCapture = TRIM(source.ProviderUsingPKChargeCapture),
 target.ProviderUsingECW = TRIM(source.ProviderUsingECW),
 target.ProviderUsingECWEMR = TRIM(source.ProviderUsingECWEMR),
 target.ProviderUsingEPIC = TRIM(source.ProviderUsingEPIC),
 target.ProviderNonStandardEMR = TRIM(source.ProviderNonStandardEMR),
 target.ProviderNonStandardSystem = TRIM(source.ProviderNonStandardSystem),
 target.ProviderWorkflowChangeDate = source.ProviderWorkflowChangeDate,
 target.ProviderEndDateComment = TRIM(source.ProviderEndDateComment),
 target.ProviderProviderLimitedSvc = TRIM(source.ProviderProviderLimitedSvc),
 target.ProviderServicesCodedbyVendor = TRIM(source.ProviderServicesCodedbyVendor),
 target.ProviderVendorName = TRIM(source.ProviderVendorName),
 target.ProviderVendorPlaced = TRIM(source.ProviderVendorPlaced),
 target.ProviderVendorAssignedDate = source.ProviderVendorAssignedDate,
 target.ProviderVendorEndDate = source.ProviderVendorEndDate,
 target.ProviderVendorPickupDate = source.ProviderVendorPickupDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM
WHEN NOT MATCHED THEN
  INSERT (CCUCoidProviderKey, Coid, CoidName, CoidNameOnly, GLDepartmentNumber, LookupValue, CenterDescription, GroupName, DivisionName, MarketName, CoidStartDateKey, CoidTermDateKey, CoidLOB, CoidSublob, CoidSpecialty, GmeCoid, EcwCoid, PKCoid, EpicCoid, PVCoid, CoidSystem, PPMSFlag, CoidConsolidationDate, CCUDiscontinuedDate, PKActivationStatus, PKEnvironment, CoidCount, ActivityCoid, ArClaimCountCoid, EwocCountCoid, CoidCentralizedStatus, CoidStatus, CoidLevelOfCentralization, PartialCentralizationReason, CoidOptsOutCCU, CoidPOSVariation, CoidPOSVariationLabel, CoidTOSVariation, CoidTOSVariationLabel, CoidWorkFlow, CoidWorkFlowCount, ProviderNPI, ProviderName, SpecialtyName, ProviderStartDateKey, ProviderTermDateKey, ProviderFutureTermDateKey, ProviderStatus, ProviderCentralizedStatus, ProviderOptsOutCCU, ProviderOptsOutCCUCoid, ProviderGroupAssignment, ProviderPOSVariation, ProviderPOSVariationLabel, ProviderTOSVariation, ProviderTOSVariationLabel, ProviderCoidPOSVariation, ProviderCoidPOSVariationLabel, ProviderCoidTOSVariation, ProviderCoidTOSVariationLabel, ProviderWorkFlow, ProviderWorkFlowCount, ProviderActivity, ProviderArClaimCount, ProviderEwocEncounterCount, MultipleCoidAssociation, ProviderCountActive, ProviderCountTermed, ProviderCountArchiveClosed, ProviderCountNA, FTE, CoidExclusionFlag, MORSnapShotDateKey, PaperProcess, PaperProcessDetail, EmbeddedCoder, DedicatedCoder, MissingDocVariation, MissingDocProcessVariation, MissingDocCategorySharepointArtiva, CodeChangeVariation, CodeChangeVariationDetail, ChargeOverride, SelfCoding, `100PercentAudit`, CCUPerformsChargeEntry, CCUAssignsProviderTVU, NotUsingECWResourceSchedule, NonHCAFacilities, ProviderUsingPKChargeCapture, ProviderUsingECW, ProviderUsingECWEMR, ProviderUsingEPIC, ProviderNonStandardEMR, ProviderNonStandardSystem, ProviderWorkflowChangeDate, ProviderEndDateComment, ProviderProviderLimitedSvc, ProviderServicesCodedbyVendor, ProviderVendorName, ProviderVendorPlaced, ProviderVendorAssignedDate, ProviderVendorEndDate, ProviderVendorPickupDate, DWLastUpdateDateTime, InsertedBy, InsertedDTM)
  VALUES (source.CCUCoidProviderKey, TRIM(source.Coid), TRIM(source.CoidName), TRIM(source.CoidNameOnly), TRIM(source.GLDepartmentNumber), TRIM(source.LookupValue), TRIM(source.CenterDescription), TRIM(source.GroupName), TRIM(source.DivisionName), TRIM(source.MarketName), source.CoidStartDateKey, source.CoidTermDateKey, TRIM(source.CoidLOB), TRIM(source.CoidSublob), TRIM(source.CoidSpecialty), TRIM(source.GmeCoid), TRIM(source.EcwCoid), TRIM(source.PKCoid), TRIM(source.EpicCoid), TRIM(source.PVCoid), TRIM(source.CoidSystem), source.PPMSFlag, TRIM(source.CoidConsolidationDate), TRIM(source.CCUDiscontinuedDate), TRIM(source.PKActivationStatus), TRIM(source.PKEnvironment), source.CoidCount, TRIM(source.ActivityCoid), source.ArClaimCountCoid, source.EwocCountCoid, TRIM(source.CoidCentralizedStatus), TRIM(source.CoidStatus), TRIM(source.CoidLevelOfCentralization), TRIM(source.PartialCentralizationReason), TRIM(source.CoidOptsOutCCU), TRIM(source.CoidPOSVariation), TRIM(source.CoidPOSVariationLabel), TRIM(source.CoidTOSVariation), TRIM(source.CoidTOSVariationLabel), TRIM(source.CoidWorkFlow), source.CoidWorkFlowCount, TRIM(source.ProviderNPI), TRIM(source.ProviderName), TRIM(source.SpecialtyName), source.ProviderStartDateKey, source.ProviderTermDateKey, source.ProviderFutureTermDateKey, TRIM(source.ProviderStatus), TRIM(source.ProviderCentralizedStatus), TRIM(source.ProviderOptsOutCCU), TRIM(source.ProviderOptsOutCCUCoid), TRIM(source.ProviderGroupAssignment), TRIM(source.ProviderPOSVariation), TRIM(source.ProviderPOSVariationLabel), TRIM(source.ProviderTOSVariation), TRIM(source.ProviderTOSVariationLabel), TRIM(source.ProviderCoidPOSVariation), TRIM(source.ProviderCoidPOSVariationLabel), TRIM(source.ProviderCoidTOSVariation), TRIM(source.ProviderCoidTOSVariationLabel), TRIM(source.ProviderWorkFlow), source.ProviderWorkFlowCount, TRIM(source.ProviderActivity), source.ProviderArClaimCount, source.ProviderEwocEncounterCount, TRIM(source.MultipleCoidAssociation), source.ProviderCountActive, source.ProviderCountTermed, source.ProviderCountArchiveClosed, source.ProviderCountNA, source.FTE, TRIM(source.CoidExclusionFlag), source.MORSnapShotDateKey, TRIM(source.PaperProcess), TRIM(source.PaperProcessDetail), TRIM(source.EmbeddedCoder), TRIM(source.DedicatedCoder), TRIM(source.MissingDocVariation), TRIM(source.MissingDocProcessVariation), TRIM(source.MissingDocCategorySharepointArtiva), TRIM(source.CodeChangeVariation), TRIM(source.CodeChangeVariationDetail), TRIM(source.ChargeOverride), TRIM(source.SelfCoding), TRIM(source.`100PercentAudit`), TRIM(source.CCUPerformsChargeEntry), TRIM(source.CCUAssignsProviderTVU), TRIM(source.NotUsingECWResourceSchedule), TRIM(source.NonHCAFacilities), TRIM(source.ProviderUsingPKChargeCapture), TRIM(source.ProviderUsingECW), TRIM(source.ProviderUsingECWEMR), TRIM(source.ProviderUsingEPIC), TRIM(source.ProviderNonStandardEMR), TRIM(source.ProviderNonStandardSystem), source.ProviderWorkflowChangeDate, TRIM(source.ProviderEndDateComment), TRIM(source.ProviderProviderLimitedSvc), TRIM(source.ProviderServicesCodedbyVendor), TRIM(source.ProviderVendorName), TRIM(source.ProviderVendorPlaced), source.ProviderVendorAssignedDate, source.ProviderVendorEndDate, source.ProviderVendorPickupDate, source.DWLastUpdateDateTime, TRIM(source.InsertedBy), source.InsertedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT CCUCoidProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.CCU_FactCoidProvider
      GROUP BY CCUCoidProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.CCU_FactCoidProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
