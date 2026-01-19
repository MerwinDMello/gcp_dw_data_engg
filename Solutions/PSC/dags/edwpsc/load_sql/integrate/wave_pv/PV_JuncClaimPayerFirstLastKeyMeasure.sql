
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_JuncClaimPayerFirstLastKeyMeasure ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_JuncClaimPayerFirstLastKeyMeasure (ClaimPayerKey, ClaimKey, Coid, PayerFirstBillKey, PayerFirstBillDateKey, PayerLastBillKey, PayerLastBillDateKey, PayerNumberOfBills, PayerFirstClaimPaymentKey, PayerFirstClaimPaymentDateKey, PayerLastClaimPaymentKey, PayerLastClaimPaymentDateKey, PayerTotalPayments, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM)
SELECT source.ClaimPayerKey, source.ClaimKey, TRIM(source.Coid), source.PayerFirstBillKey, source.PayerFirstBillDateKey, source.PayerLastBillKey, source.PayerLastBillDateKey, source.PayerNumberOfBills, source.PayerFirstClaimPaymentKey, source.PayerFirstClaimPaymentDateKey, source.PayerLastClaimPaymentKey, source.PayerLastClaimPaymentDateKey, source.PayerTotalPayments, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM
FROM {{ params.param_psc_stage_dataset_name }}.PV_JuncClaimPayerFirstLastKeyMeasure as source;
