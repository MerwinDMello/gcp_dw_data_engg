Locking table  EDWPBS_Staging.Stg_CR_Refund_Transmitted for access
SELECT 'PBDCR002-10' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM EDWPBS_Staging.Stg_CR_Refund_Transmitted Stg