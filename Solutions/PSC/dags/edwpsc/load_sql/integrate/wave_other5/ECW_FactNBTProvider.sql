
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_FactNBTProvider AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_FactNBTProvider AS source
ON target.NBTProviderKey = source.NBTProviderKey
WHEN MATCHED THEN
  UPDATE SET
  target.NBTProviderKey = source.NBTProviderKey,
 target.SnapShotDateKey = source.SnapShotDateKey,
 target.MonthId = source.MonthId,
 target.HCPDWId = source.HCPDWId,
 target.GLDepartmentNum = TRIM(source.GLDepartmentNum),
 target.COID = TRIM(source.COID),
 target.CompanyCode = TRIM(source.CompanyCode),
 target.AncillaryDeptNum = TRIM(source.AncillaryDeptNum),
 target.AssignedStartDateKey = source.AssignedStartDateKey,
 target.BudgetCOID = TRIM(source.BudgetCOID),
 target.BudgetOHDeptNum = TRIM(source.BudgetOHDeptNum),
 target.BudgetStartDateKey = source.BudgetStartDateKey,
 target.COIDDeptName = TRIM(source.COIDDeptName),
 target.COIDDeptStartDateKey = source.COIDDeptStartDateKey,
 target.COIDDeptTermDateKey = source.COIDDeptTermDateKey,
 target.COIDStartDateKey = source.COIDStartDateKey,
 target.CommunityStatus = TRIM(source.CommunityStatus),
 target.CompensationType = TRIM(source.CompensationType),
 target.SrcSysCompensationTypeKey = source.SrcSysCompensationTypeKey,
 target.ContractExpirationDateKey = source.ContractExpirationDateKey,
 target.ContractRenewalDateKey = source.ContractRenewalDateKey,
 target.DevelopmentTypeName = TRIM(source.DevelopmentTypeName),
 target.SrcSysDevelopmentKey = source.SrcSysDevelopmentKey,
 target.BirthDateKey = source.BirthDateKey,
 target.EffectiveDateKey = source.EffectiveDateKey,
 target.ProviderFirstName = TRIM(source.ProviderFirstName),
 target.FTE = source.FTE,
 target.IsBudgetInd = TRIM(source.IsBudgetInd),
 target.IsHospitalistInd = TRIM(source.IsHospitalistInd),
 target.IsMultipleProviderDeptInd = TRIM(source.IsMultipleProviderDeptInd),
 target.ProviderLastName = TRIM(source.ProviderLastName),
 target.ProviderMiddleInitial = TRIM(source.ProviderMiddleInitial),
 target.NewReplacedInd = TRIM(source.NewReplacedInd),
 target.NewReplacedName = TRIM(source.NewReplacedName),
 target.OverHeadAllocation = TRIM(source.OverHeadAllocation),
 target.OriginalStartDateKey = source.OriginalStartDateKey,
 target.ProviderNPI = TRIM(source.ProviderNPI),
 target.ProviderKey = source.ProviderKey,
 target.PracticeAddress1 = TRIM(source.PracticeAddress1),
 target.PracticeAddress2 = TRIM(source.PracticeAddress2),
 target.PracticeCity = TRIM(source.PracticeCity),
 target.PracticeFax = TRIM(source.PracticeFax),
 target.PracticeManagerFirstName = TRIM(source.PracticeManagerFirstName),
 target.PracticeManagerLastName = TRIM(source.PracticeManagerLastName),
 target.PracticeName = TRIM(source.PracticeName),
 target.SrcSysPracticeNum = source.SrcSysPracticeNum,
 target.PracticePhone = TRIM(source.PracticePhone),
 target.PracticeState = TRIM(source.PracticeState),
 target.PracticeZip = TRIM(source.PracticeZip),
 target.ProjectedTerminationDateKey = source.ProjectedTerminationDateKey,
 target.ProviderHeadCount = source.ProviderHeadCount,
 target.SrcSysProviderKey = source.SrcSysProviderKey,
 target.RelationshipInd = source.RelationshipInd,
 target.SocialSecurityNum = TRIM(source.SocialSecurityNum),
 target.SpecialtyCode = TRIM(source.SpecialtyCode),
 target.SpecialtyName = TRIM(source.SpecialtyName),
 target.SpecialtyType = TRIM(source.SpecialtyType),
 target.TerminationReasonName = TRIM(source.TerminationReasonName),
 target.TerminationTypeName = TRIM(source.TerminationTypeName),
 target.TerminationTypeId = source.TerminationTypeId,
 target.TerminationResults = TRIM(source.TerminationResults),
 target.UPIN = TRIM(source.UPIN),
 target.Status = source.Status,
 target.DataSourceCode = TRIM(source.DataSourceCode),
 target.CreatedDTM = source.CreatedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourceSystemCode = TRIM(source.SourceSystemCode),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM
WHEN NOT MATCHED THEN
  INSERT (NBTProviderKey, SnapShotDateKey, MonthId, HCPDWId, GLDepartmentNum, COID, CompanyCode, AncillaryDeptNum, AssignedStartDateKey, BudgetCOID, BudgetOHDeptNum, BudgetStartDateKey, COIDDeptName, COIDDeptStartDateKey, COIDDeptTermDateKey, COIDStartDateKey, CommunityStatus, CompensationType, SrcSysCompensationTypeKey, ContractExpirationDateKey, ContractRenewalDateKey, DevelopmentTypeName, SrcSysDevelopmentKey, BirthDateKey, EffectiveDateKey, ProviderFirstName, FTE, IsBudgetInd, IsHospitalistInd, IsMultipleProviderDeptInd, ProviderLastName, ProviderMiddleInitial, NewReplacedInd, NewReplacedName, OverHeadAllocation, OriginalStartDateKey, ProviderNPI, ProviderKey, PracticeAddress1, PracticeAddress2, PracticeCity, PracticeFax, PracticeManagerFirstName, PracticeManagerLastName, PracticeName, SrcSysPracticeNum, PracticePhone, PracticeState, PracticeZip, ProjectedTerminationDateKey, ProviderHeadCount, SrcSysProviderKey, RelationshipInd, SocialSecurityNum, SpecialtyCode, SpecialtyName, SpecialtyType, TerminationReasonName, TerminationTypeName, TerminationTypeId, TerminationResults, UPIN, Status, DataSourceCode, CreatedDTM, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
  VALUES (source.NBTProviderKey, source.SnapShotDateKey, source.MonthId, source.HCPDWId, TRIM(source.GLDepartmentNum), TRIM(source.COID), TRIM(source.CompanyCode), TRIM(source.AncillaryDeptNum), source.AssignedStartDateKey, TRIM(source.BudgetCOID), TRIM(source.BudgetOHDeptNum), source.BudgetStartDateKey, TRIM(source.COIDDeptName), source.COIDDeptStartDateKey, source.COIDDeptTermDateKey, source.COIDStartDateKey, TRIM(source.CommunityStatus), TRIM(source.CompensationType), source.SrcSysCompensationTypeKey, source.ContractExpirationDateKey, source.ContractRenewalDateKey, TRIM(source.DevelopmentTypeName), source.SrcSysDevelopmentKey, source.BirthDateKey, source.EffectiveDateKey, TRIM(source.ProviderFirstName), source.FTE, TRIM(source.IsBudgetInd), TRIM(source.IsHospitalistInd), TRIM(source.IsMultipleProviderDeptInd), TRIM(source.ProviderLastName), TRIM(source.ProviderMiddleInitial), TRIM(source.NewReplacedInd), TRIM(source.NewReplacedName), TRIM(source.OverHeadAllocation), source.OriginalStartDateKey, TRIM(source.ProviderNPI), source.ProviderKey, TRIM(source.PracticeAddress1), TRIM(source.PracticeAddress2), TRIM(source.PracticeCity), TRIM(source.PracticeFax), TRIM(source.PracticeManagerFirstName), TRIM(source.PracticeManagerLastName), TRIM(source.PracticeName), source.SrcSysPracticeNum, TRIM(source.PracticePhone), TRIM(source.PracticeState), TRIM(source.PracticeZip), source.ProjectedTerminationDateKey, source.ProviderHeadCount, source.SrcSysProviderKey, source.RelationshipInd, TRIM(source.SocialSecurityNum), TRIM(source.SpecialtyCode), TRIM(source.SpecialtyName), TRIM(source.SpecialtyType), TRIM(source.TerminationReasonName), TRIM(source.TerminationTypeName), source.TerminationTypeId, TRIM(source.TerminationResults), TRIM(source.UPIN), source.Status, TRIM(source.DataSourceCode), source.CreatedDTM, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT NBTProviderKey
      FROM {{ params.param_psc_core_dataset_name }}.ECW_FactNBTProvider
      GROUP BY NBTProviderKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_FactNBTProvider');
ELSE
  COMMIT TRANSACTION;
END IF;
