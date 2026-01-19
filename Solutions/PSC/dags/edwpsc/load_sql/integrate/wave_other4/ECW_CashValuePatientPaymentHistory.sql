TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_CashValuePatientPaymentHistory;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_CashValuePatientPaymentHistory
  (
    PatientBillType,
    Coid,
    DeptCode,
    MarketKey,
    SpecialtyCategoryKey,
    RenderingProviderKey,
    GuarantorPatientKey,
    ClaimKey,
    FirstPatientStatementBillDateKey,
    LastPatientStatementBillDateKey,
    NumberOfPatientStatements,
    LastPatientClaimPaymentDateKey,
    BankDaysToPayment,
    TotalPatientResponsibilityAmt,
    BalanceBilledToPatient,
    UpfrontPaymentAmt,
    PostBillPaymentAmt,
    TotalPatientPaymentsAmt,
    TotalPatientBalanceAmt,
    DWLastUpdateDateTime)
SELECT
  TRIM(source.PatientBillType),
  TRIM(source.Coid),
  TRIM(source.DeptCode),
  source.MarketKey,
  source.SpecialtyCategoryKey,
  source.RenderingProviderKey,
  source.GuarantorPatientKey,
  source.ClaimKey,
  source.FirstPatientStatementBillDateKey,
  source.LastPatientStatementBillDateKey,
  source.NumberOfPatientStatements,
  source.LastPatientClaimPaymentDateKey,
  source.BankDaysToPayment,
  source.TotalPatientResponsibilityAmt,
  source.BalanceBilledToPatient,
  source.UpfrontPaymentAmt,
  source.PostBillPaymentAmt,
  source.TotalPatientPaymentsAmt,
  source.TotalPatientBalanceAmt,
  source.DWLastUpdateDateTime
FROM
  {{ params.param_psc_stage_dataset_name }}.ECW_CashValuePatientPaymentHistory
    AS source;
