
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_RprtARCompMOR AS target
USING {{ params.param_psc_stage_dataset_name }}.ECW_RprtARCompMOR AS source
ON target.SnapShotDate = source.SnapShotDate
WHEN MATCHED THEN
  UPDATE SET
  target.AvgDGR3Mo = source.AvgDGR3Mo,
 target.BalanceType = TRIM(source.BalanceType),
 target.Center = TRIM(source.Center),
 target.CohOther = source.CohOther,
 target.CohPE = source.CohPE,
 target.Coid = TRIM(source.Coid),
 target.CoidName = TRIM(source.CoidName),
 target.GLDepartmentNumber = TRIM(source.GLDepartmentNumber),
 target.HospitalName = TRIM(source.HospitalName),
 target.HospitalCoid = TRIM(source.HospitalCoid),
 target.CoidSpecialty = TRIM(source.CoidSpecialty),
 target.DaysInLast3Months = source.DaysInLast3Months,
 target.DivisionName = TRIM(source.DivisionName),
 target.GroupName = TRIM(source.GroupName),
 target.Ins1InsurancePayerGroupName = TRIM(source.Ins1InsurancePayerGroupName),
 target.InsuranceArBilled = source.InsuranceArBilled,
 target.InsuranceArUnbilledEcw = source.InsuranceArUnbilledEcw,
 target.InsuranceArUnbilledRH = source.InsuranceArUnbilledRH,
 target.LeftOver = source.LeftOver,
 target.LOB = TRIM(source.LOB),
 target.MarketName = TRIM(source.MarketName),
 target.PatientUnidentified = source.PatientUnidentified,
 target.ServicingProviderName = TRIM(source.ServicingProviderName),
 target.ServicingProviderNPI = TRIM(source.ServicingProviderNPI),
 target.ServicingProviderTaxonomyCode = TRIM(source.ServicingProviderTaxonomyCode),
 target.State = TRIM(source.State),
 target.SubLOB = TRIM(source.SubLOB),
 target.SYSTEM = TRIM(source.SYSTEM),
 target.TotalBalance = source.TotalBalance,
 target.TotalCharges = source.TotalCharges,
 target.TotalChargesLast3Months = source.TotalChargesLast3Months,
 target.TotalInsuranceBalance = source.TotalInsuranceBalance,
 target.TotalPatientBalance = source.TotalPatientBalance,
 target.ClaimAgingBucket = TRIM(source.ClaimAgingBucket),
 target.PatientCollections = source.PatientCollections,
 target.PatientPSC = source.PatientPSC,
 target.CoidStartMonth = source.CoidStartMonth,
 target.CoidTermMonth = source.CoidTermMonth,
 target.ServicingProviderStartMonth = source.ServicingProviderStartMonth,
 target.ServicingProviderTermMonth = source.ServicingProviderTermMonth,
 target.ServicingProviderSpecialty = TRIM(source.ServicingProviderSpecialty),
 target.ServicingProviderSubSpecialty = TRIM(source.ServicingProviderSubSpecialty),
 target.Ins1FinancialClassNum = TRIM(source.Ins1FinancialClassNum),
 target.Ins1FinancialClassName = TRIM(source.Ins1FinancialClassName),
 target.InsertedBy = TRIM(source.InsertedBy),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedBy = TRIM(source.ModifiedBy),
 target.ModifiedDTM = source.ModifiedDTM,
 target.ClaimCount = source.ClaimCount,
 target.SnapShotDate = source.SnapShotDate,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime
WHEN NOT MATCHED THEN
  INSERT (AvgDGR3Mo, BalanceType, Center, CohOther, CohPE, Coid, CoidName, GLDepartmentNumber, HospitalName, HospitalCoid, CoidSpecialty, DaysInLast3Months, DivisionName, GroupName, Ins1InsurancePayerGroupName, InsuranceArBilled, InsuranceArUnbilledEcw, InsuranceArUnbilledRH, LeftOver, LOB, MarketName, PatientUnidentified, ServicingProviderName, ServicingProviderNPI, ServicingProviderTaxonomyCode, State, SubLOB, SYSTEM, TotalBalance, TotalCharges, TotalChargesLast3Months, TotalInsuranceBalance, TotalPatientBalance, ClaimAgingBucket, PatientCollections, PatientPSC, CoidStartMonth, CoidTermMonth, ServicingProviderStartMonth, ServicingProviderTermMonth, ServicingProviderSpecialty, ServicingProviderSubSpecialty, Ins1FinancialClassNum, Ins1FinancialClassName, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, ClaimCount, SnapShotDate, DWLastUpdateDateTime)
  VALUES (source.AvgDGR3Mo, TRIM(source.BalanceType), TRIM(source.Center), source.CohOther, source.CohPE, TRIM(source.Coid), TRIM(source.CoidName), TRIM(source.GLDepartmentNumber), TRIM(source.HospitalName), TRIM(source.HospitalCoid), TRIM(source.CoidSpecialty), source.DaysInLast3Months, TRIM(source.DivisionName), TRIM(source.GroupName), TRIM(source.Ins1InsurancePayerGroupName), source.InsuranceArBilled, source.InsuranceArUnbilledEcw, source.InsuranceArUnbilledRH, source.LeftOver, TRIM(source.LOB), TRIM(source.MarketName), source.PatientUnidentified, TRIM(source.ServicingProviderName), TRIM(source.ServicingProviderNPI), TRIM(source.ServicingProviderTaxonomyCode), TRIM(source.State), TRIM(source.SubLOB), TRIM(source.SYSTEM), source.TotalBalance, source.TotalCharges, source.TotalChargesLast3Months, source.TotalInsuranceBalance, source.TotalPatientBalance, TRIM(source.ClaimAgingBucket), source.PatientCollections, source.PatientPSC, source.CoidStartMonth, source.CoidTermMonth, source.ServicingProviderStartMonth, source.ServicingProviderTermMonth, TRIM(source.ServicingProviderSpecialty), TRIM(source.ServicingProviderSubSpecialty), TRIM(source.Ins1FinancialClassNum), TRIM(source.Ins1FinancialClassName), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.ClaimCount, source.SnapShotDate, source.DWLastUpdateDateTime);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT SnapShotDate
      FROM {{ params.param_psc_core_dataset_name }}.ECW_RprtARCompMOR
      GROUP BY SnapShotDate
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_RprtARCompMOR');
ELSE
  COMMIT TRANSACTION;
END IF;
