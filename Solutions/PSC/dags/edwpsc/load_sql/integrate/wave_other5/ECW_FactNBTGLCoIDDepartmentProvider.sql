
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactNBTGLCoIDDepartmentProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactNBTGLCoIDDepartmentProvider AS source
ON target.NBTGLCoIDDepartmentProviderKey = source.NBTGLCoIDDepartmentProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.NBTGLCoIDDepartmentProviderKey = source.NBTGLCoIDDepartmentProviderKey,
 target.GLPeriod = TRIM(source.GLPeriod),
 target.CoID = source.CoID,
 target.ProviderDepartmentNo = source.ProviderDepartmentNo,
 target.CoIDDepartmentProviderRelationship = TRIM(source.CoIDDepartmentProviderRelationship),
 target.CoIDDepartmentProviderSpecialtyCode = TRIM(source.CoIDDepartmentProviderSpecialtyCode),
 target.CoIDDepartmentProviderSpecialtyDescription = TRIM(source.CoIDDepartmentProviderSpecialtyDescription),
 target.CoIDDepartmentProviderSpecialtyType = TRIM(source.CoIDDepartmentProviderSpecialtyType),
 target.CoIDDepartmentRelationship = TRIM(source.CoIDDepartmentRelationship),
 target.CoIDDepartmentSpecialtyCode = TRIM(source.CoIDDepartmentSpecialtyCode),
 target.CoIDDepartmentSpecialtyDescription = TRIM(source.CoIDDepartmentSpecialtyDescription),
 target.CoIDDepartmentSpecialtyType = TRIM(source.CoIDDepartmentSpecialtyType),
 target.CoIDDepartmentCompensationType = TRIM(source.CoIDDepartmentCompensationType),
 target.CoIDDepartmentIsApproved = source.CoIDDepartmentIsApproved,
 target.CoIDDepartmentIsBudgetCC = source.CoIDDepartmentIsBudgetCC,
 target.CoIDDepartmentName = TRIM(source.CoIDDepartmentName),
 target.CoIDDepartmentProviderAssignedStartDate = source.CoIDDepartmentProviderAssignedStartDate,
 target.CoIDDepartmentProviderIsApproved = source.CoIDDepartmentProviderIsApproved,
 target.CoIDDepartmentProviderLastUpdateDate = source.CoIDDepartmentProviderLastUpdateDate,
 target.CoIDDepartmentProviderLastUpdatedByUserId = TRIM(source.CoIDDepartmentProviderLastUpdatedByUserId),
 target.CoIDDepartmentProviderStatus = TRIM(source.CoIDDepartmentProviderStatus),
 target.CoIDDepartmentProviderStatusChangeDate = source.CoIDDepartmentProviderStatusChangeDate,
 target.CoIDDepartmentStartDate = source.CoIDDepartmentStartDate,
 target.AncillaryDepartmentNo = source.AncillaryDepartmentNo,
 target.BudgetStartDate = source.BudgetStartDate,
 target.CommunityStatus = TRIM(source.CommunityStatus),
 target.ContractPaysConversionDate = source.ContractPaysConversionDate,
 target.ContractCompensationType = TRIM(source.ContractCompensationType),
 target.ContractCurrentlyPays = TRIM(source.ContractCurrentlyPays),
 target.ContractEffectiveDate = source.ContractEffectiveDate,
 target.ContractExpirationDate = source.ContractExpirationDate,
 target.FTE = source.FTE,
 target.IsHospitalist = source.IsHospitalist,
 target.IsMultipleProviderDepartment = source.IsMultipleProviderDepartment,
 target.NewReplacement = TRIM(source.NewReplacement),
 target.OverheadDepartmentNo = source.OverheadDepartmentNo,
 target.PracticeEstablishedThrough = TRIM(source.PracticeEstablishedThrough),
 target.ProductivityReason = TRIM(source.ProductivityReason),
 target.ProductivityLevelFuture = TRIM(source.ProductivityLevelFuture),
 target.ProductivityLevelToday = TRIM(source.ProductivityLevelToday),
 target.ProductivityStatus = TRIM(source.ProductivityStatus),
 target.DateChangedToProductivity = source.DateChangedToProductivity,
 target.TechnicalLevel = TRIM(source.TechnicalLevel),
 target.ProjectedTerminationDate = source.ProjectedTerminationDate,
 target.ProviderStrategyDescription = TRIM(source.ProviderStrategyDescription),
 target.ProviderStrategyTypeReason = TRIM(source.ProviderStrategyTypeReason),
 target.TerminationReason = TRIM(source.TerminationReason),
 target.TerminationResult = TRIM(source.TerminationResult),
 target.TerminationType = TRIM(source.TerminationType),
 target.UPIN = TRIM(source.UPIN),
 target.PracticeId = source.PracticeId,
 target.ProviderId = source.ProviderId,
 target.CoIDDepartmentId = source.CoIDDepartmentId,
 target.CoIDDepartmentProviderId = source.CoIDDepartmentProviderId,
 target.ContractId = source.ContractId,
 target.OHAllocation = source.OHAllocation,
 target.AllocationTypeDescription = TRIM(source.AllocationTypeDescription),
 target.CoIDDepartmentProviderStatusId = source.CoIDDepartmentProviderStatusId,
 target.CoIDDepartmentCompensationTypeId = source.CoIDDepartmentCompensationTypeId,
 target.ProviderDeviationStatusId = TRIM(source.ProviderDeviationStatusId),
 target.ProviderDeviationStatusDescription = TRIM(source.ProviderDeviationStatusDescription),
 target.MORProviderStatusId = source.MORProviderStatusId,
 target.MORProviderStatus = TRIM(source.MORProviderStatus),
 target.MORProviderStatusDetailId = source.MORProviderStatusDetailId,
 target.MORProviderStatusDetailName = TRIM(source.MORProviderStatusDetailName),
 target.SignOnBonus = source.SignOnBonus,
 target.RelocationExpenseBonus = source.RelocationExpenseBonus,
 target.AnnualStayIncentive = source.AnnualStayIncentive,
 target.CoIDDepartmentNBTSpecialtyCategoryId = source.CoIDDepartmentNBTSpecialtyCategoryId,
 target.CoIDDepartmentNBTSpecialtyCategoryDesc = TRIM(source.CoIDDepartmentNBTSpecialtyCategoryDesc),
 target.CoIDDepartmentProviderNBTSpecialtyCategoryId = source.CoIDDepartmentProviderNBTSpecialtyCategoryId,
 target.CoIDDepartmentProviderNBTSpecialtyCategoryDesc = TRIM(source.CoIDDepartmentProviderNBTSpecialtyCategoryDesc),
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (NBTGLCoIDDepartmentProviderKey, GLPeriod, CoID, ProviderDepartmentNo, CoIDDepartmentProviderRelationship, CoIDDepartmentProviderSpecialtyCode, CoIDDepartmentProviderSpecialtyDescription, CoIDDepartmentProviderSpecialtyType, CoIDDepartmentRelationship, CoIDDepartmentSpecialtyCode, CoIDDepartmentSpecialtyDescription, CoIDDepartmentSpecialtyType, CoIDDepartmentCompensationType, CoIDDepartmentIsApproved, CoIDDepartmentIsBudgetCC, CoIDDepartmentName, CoIDDepartmentProviderAssignedStartDate, CoIDDepartmentProviderIsApproved, CoIDDepartmentProviderLastUpdateDate, CoIDDepartmentProviderLastUpdatedByUserId, CoIDDepartmentProviderStatus, CoIDDepartmentProviderStatusChangeDate, CoIDDepartmentStartDate, AncillaryDepartmentNo, BudgetStartDate, CommunityStatus, ContractPaysConversionDate, ContractCompensationType, ContractCurrentlyPays, ContractEffectiveDate, ContractExpirationDate, FTE, IsHospitalist, IsMultipleProviderDepartment, NewReplacement, OverheadDepartmentNo, PracticeEstablishedThrough, ProductivityReason, ProductivityLevelFuture, ProductivityLevelToday, ProductivityStatus, DateChangedToProductivity, TechnicalLevel, ProjectedTerminationDate, ProviderStrategyDescription, ProviderStrategyTypeReason, TerminationReason, TerminationResult, TerminationType, UPIN, PracticeId, ProviderId, CoIDDepartmentId, CoIDDepartmentProviderId, ContractId, OHAllocation, AllocationTypeDescription, CoIDDepartmentProviderStatusId, CoIDDepartmentCompensationTypeId, ProviderDeviationStatusId, ProviderDeviationStatusDescription, MORProviderStatusId, MORProviderStatus, MORProviderStatusDetailId, MORProviderStatusDetailName, SignOnBonus, RelocationExpenseBonus, AnnualStayIncentive, CoIDDepartmentNBTSpecialtyCategoryId, CoIDDepartmentNBTSpecialtyCategoryDesc, CoIDDepartmentProviderNBTSpecialtyCategoryId, CoIDDepartmentProviderNBTSpecialtyCategoryDesc, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.NBTGLCoIDDepartmentProviderKey, TRIM(source.GLPeriod), source.CoID, source.ProviderDepartmentNo, TRIM(source.CoIDDepartmentProviderRelationship), TRIM(source.CoIDDepartmentProviderSpecialtyCode), TRIM(source.CoIDDepartmentProviderSpecialtyDescription), TRIM(source.CoIDDepartmentProviderSpecialtyType), TRIM(source.CoIDDepartmentRelationship), TRIM(source.CoIDDepartmentSpecialtyCode), TRIM(source.CoIDDepartmentSpecialtyDescription), TRIM(source.CoIDDepartmentSpecialtyType), TRIM(source.CoIDDepartmentCompensationType), source.CoIDDepartmentIsApproved, source.CoIDDepartmentIsBudgetCC, TRIM(source.CoIDDepartmentName), source.CoIDDepartmentProviderAssignedStartDate, source.CoIDDepartmentProviderIsApproved, source.CoIDDepartmentProviderLastUpdateDate, TRIM(source.CoIDDepartmentProviderLastUpdatedByUserId), TRIM(source.CoIDDepartmentProviderStatus), source.CoIDDepartmentProviderStatusChangeDate, source.CoIDDepartmentStartDate, source.AncillaryDepartmentNo, source.BudgetStartDate, TRIM(source.CommunityStatus), source.ContractPaysConversionDate, TRIM(source.ContractCompensationType), TRIM(source.ContractCurrentlyPays), source.ContractEffectiveDate, source.ContractExpirationDate, source.FTE, source.IsHospitalist, source.IsMultipleProviderDepartment, TRIM(source.NewReplacement), source.OverheadDepartmentNo, TRIM(source.PracticeEstablishedThrough), TRIM(source.ProductivityReason), TRIM(source.ProductivityLevelFuture), TRIM(source.ProductivityLevelToday), TRIM(source.ProductivityStatus), source.DateChangedToProductivity, TRIM(source.TechnicalLevel), source.ProjectedTerminationDate, TRIM(source.ProviderStrategyDescription), TRIM(source.ProviderStrategyTypeReason), TRIM(source.TerminationReason), TRIM(source.TerminationResult), TRIM(source.TerminationType), TRIM(source.UPIN), source.PracticeId, source.ProviderId, source.CoIDDepartmentId, source.CoIDDepartmentProviderId, source.ContractId, source.OHAllocation, TRIM(source.AllocationTypeDescription), source.CoIDDepartmentProviderStatusId, source.CoIDDepartmentCompensationTypeId, TRIM(source.ProviderDeviationStatusId), TRIM(source.ProviderDeviationStatusDescription), source.MORProviderStatusId, TRIM(source.MORProviderStatus), source.MORProviderStatusDetailId, TRIM(source.MORProviderStatusDetailName), source.SignOnBonus, source.RelocationExpenseBonus, source.AnnualStayIncentive, source.CoIDDepartmentNBTSpecialtyCategoryId, TRIM(source.CoIDDepartmentNBTSpecialtyCategoryDesc), source.CoIDDepartmentProviderNBTSpecialtyCategoryId, TRIM(source.CoIDDepartmentProviderNBTSpecialtyCategoryDesc), source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NBTGLCoIDDepartmentProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactNBTGLCoIDDepartmentProvider
      GROUP BY NBTGLCoIDDepartmentProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactNBTGLCoIDDepartmentProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
