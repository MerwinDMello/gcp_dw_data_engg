Locking table  EDWPBS_Staging.Stg_CR_Balance_Refund_Header for access
SELECT 'PBDCR002-30' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM EDWPBS_Staging.Stg_CR_Balance_Refund_Header Stg