
        
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.PV_FactClaimPaymentCreditCardTransaction ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.PV_FactClaimPaymentCreditCardTransaction (ClaimKey, LogDetailPK, CreditCardType, CCSaleType, CreatedCardDate, ReserveAmt, ReserveAmtUsed, ReserveAmtRemaining, DeletedFlag, DeclinedOrCancelledFlag, DeclinedAmt, RegionKey, DWLastUpdateDateTime, SourceSystemCode, InsertedBy, InsertedDTM, ModifiedBy, ModifiedDTM, LoadKey)
SELECT source.ClaimKey, TRIM(source.LogDetailPK), TRIM(source.CreditCardType), TRIM(source.CCSaleType), source.CreatedCardDate, source.ReserveAmt, source.ReserveAmtUsed, source.ReserveAmtRemaining, source.DeletedFlag, source.DeclinedOrCancelledFlag, source.DeclinedAmt, source.RegionKey, source.DWLastUpdateDateTime, TRIM(source.SourceSystemCode), TRIM(source.InsertedBy), source.InsertedDTM, TRIM(source.ModifiedBy), source.ModifiedDTM, source.LoadKey
FROM {{ params.param_psc_stage_dataset_name }}.PV_FactClaimPaymentCreditCardTransaction as source;
