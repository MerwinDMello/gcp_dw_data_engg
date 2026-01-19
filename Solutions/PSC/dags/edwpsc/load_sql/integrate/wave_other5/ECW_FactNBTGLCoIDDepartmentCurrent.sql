
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactNBTGLCoIDDepartmentCurrent AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactNBTGLCoIDDepartmentCurrent AS source
ON target.NBTGLCoIDDepartmentCurrentKey = source.NBTGLCoIDDepartmentCurrentKey
WHEN MATCHED THEN
  UPDATE SET
  target.NBTGLCoIDDepartmentCurrentKey = source.NBTGLCoIDDepartmentCurrentKey,
 target.GLPeriod = TRIM(source.GLPeriod),
 target.CoID = source.CoID,
 target.ProviderDepartmentNo = source.ProviderDepartmentNo,
 target.OverheadDepartmentNo = source.OverheadDepartmentNo,
 target.AncillaryDepartmentNo = source.AncillaryDepartmentNo,
 target.CoIDDepartmentName = TRIM(source.CoIDDepartmentName),
 target.ProviderId = source.ProviderId,
 target.CoIDDepartmentRelationshipId = source.CoIDDepartmentRelationshipId,
 target.CoIDDepartmentRelationship = TRIM(source.CoIDDepartmentRelationship),
 target.SpecialtyId = source.SpecialtyId,
 target.CoIDDepartmentSpecialtyCode = TRIM(source.CoIDDepartmentSpecialtyCode),
 target.PracticeEstablishedId = source.PracticeEstablishedId,
 target.PracticeEstablishedThrough = TRIM(source.PracticeEstablishedThrough),
 target.BudgetStartDate = source.BudgetStartDate,
 target.CoIDDepartmentIsBudgetCC = source.CoIDDepartmentIsBudgetCC,
 target.CoIDDepartmentStartDate = source.CoIDDepartmentStartDate,
 target.ProviderOriginalStartDate = TRIM(source.ProviderOriginalStartDate),
 target.CoIDDepartmentProviderAssignedStartDate = source.CoIDDepartmentProviderAssignedStartDate,
 target.CommunityStatus = TRIM(source.CommunityStatus),
 target.CoIDDepartmentCompensationTypeId = source.CoIDDepartmentCompensationTypeId,
 target.CoIDDepartmentCompensationType = TRIM(source.CoIDDepartmentCompensationType),
 target.CoIDDepartmentFTE = source.CoIDDepartmentFTE,
 target.UPIN = TRIM(source.UPIN),
 target.NewReplacementId = source.NewReplacementId,
 target.NewReplacementDescription = TRIM(source.NewReplacementDescription),
 target.ProviderStrategyId = source.ProviderStrategyId,
 target.ProviderStrategyDescription = TRIM(source.ProviderStrategyDescription),
 target.IsHospitalist = source.IsHospitalist,
 target.IsMultipleProviderDepartment = source.IsMultipleProviderDepartment,
 target.CoIDDepartmentProviderStatusId = source.CoIDDepartmentProviderStatusId,
 target.CoIDDepartmentProviderStatus = TRIM(source.CoIDDepartmentProviderStatus),
 target.PracticeId = source.PracticeId,
 target.DevelopmentTypeId = source.DevelopmentTypeId,
 target.DevelopmentTypeDescription = TRIM(source.DevelopmentTypeDescription),
 target.ContractEffectiveDate = TRIM(source.ContractEffectiveDate),
 target.ContractCompensationType = TRIM(source.ContractCompensationType),
 target.ContractExpirationDate = source.ContractExpirationDate,
 target.ProviderHeadCount = source.ProviderHeadCount,
 target.TerminationDate = TRIM(source.TerminationDate),
 target.OHAllocation = source.OHAllocation,
 target.AllocationTypeDescription = TRIM(source.AllocationTypeDescription),
 target.CoIDStartDate = source.CoIDStartDate,
 target.OriginalProviderStatusId = source.OriginalProviderStatusId,
 target.OriginalProviderStatusDescription = TRIM(source.OriginalProviderStatusDescription),
 target.PhysicianStatusDetailId = TRIM(source.PhysicianStatusDetailId),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (NBTGLCoIDDepartmentCurrentKey, GLPeriod, CoID, ProviderDepartmentNo, OverheadDepartmentNo, AncillaryDepartmentNo, CoIDDepartmentName, ProviderId, CoIDDepartmentRelationshipId, CoIDDepartmentRelationship, SpecialtyId, CoIDDepartmentSpecialtyCode, PracticeEstablishedId, PracticeEstablishedThrough, BudgetStartDate, CoIDDepartmentIsBudgetCC, CoIDDepartmentStartDate, ProviderOriginalStartDate, CoIDDepartmentProviderAssignedStartDate, CommunityStatus, CoIDDepartmentCompensationTypeId, CoIDDepartmentCompensationType, CoIDDepartmentFTE, UPIN, NewReplacementId, NewReplacementDescription, ProviderStrategyId, ProviderStrategyDescription, IsHospitalist, IsMultipleProviderDepartment, CoIDDepartmentProviderStatusId, CoIDDepartmentProviderStatus, PracticeId, DevelopmentTypeId, DevelopmentTypeDescription, ContractEffectiveDate, ContractCompensationType, ContractExpirationDate, ProviderHeadCount, TerminationDate, OHAllocation, AllocationTypeDescription, CoIDStartDate, OriginalProviderStatusId, OriginalProviderStatusDescription, PhysicianStatusDetailId, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.NBTGLCoIDDepartmentCurrentKey, TRIM(source.GLPeriod), source.CoID, source.ProviderDepartmentNo, source.OverheadDepartmentNo, source.AncillaryDepartmentNo, TRIM(source.CoIDDepartmentName), source.ProviderId, source.CoIDDepartmentRelationshipId, TRIM(source.CoIDDepartmentRelationship), source.SpecialtyId, TRIM(source.CoIDDepartmentSpecialtyCode), source.PracticeEstablishedId, TRIM(source.PracticeEstablishedThrough), source.BudgetStartDate, source.CoIDDepartmentIsBudgetCC, source.CoIDDepartmentStartDate, TRIM(source.ProviderOriginalStartDate), source.CoIDDepartmentProviderAssignedStartDate, TRIM(source.CommunityStatus), source.CoIDDepartmentCompensationTypeId, TRIM(source.CoIDDepartmentCompensationType), source.CoIDDepartmentFTE, TRIM(source.UPIN), source.NewReplacementId, TRIM(source.NewReplacementDescription), source.ProviderStrategyId, TRIM(source.ProviderStrategyDescription), source.IsHospitalist, source.IsMultipleProviderDepartment, source.CoIDDepartmentProviderStatusId, TRIM(source.CoIDDepartmentProviderStatus), source.PracticeId, source.DevelopmentTypeId, TRIM(source.DevelopmentTypeDescription), TRIM(source.ContractEffectiveDate), TRIM(source.ContractCompensationType), source.ContractExpirationDate, source.ProviderHeadCount, TRIM(source.TerminationDate), source.OHAllocation, TRIM(source.AllocationTypeDescription), source.CoIDStartDate, source.OriginalProviderStatusId, TRIM(source.OriginalProviderStatusDescription), TRIM(source.PhysicianStatusDetailId), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NBTGLCoIDDepartmentCurrentKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactNBTGLCoIDDepartmentCurrent
      GROUP BY NBTGLCoIDDepartmentCurrentKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactNBTGLCoIDDepartmentCurrent');
ELSE
  COMMIT TRANSACTION;
END IF;
