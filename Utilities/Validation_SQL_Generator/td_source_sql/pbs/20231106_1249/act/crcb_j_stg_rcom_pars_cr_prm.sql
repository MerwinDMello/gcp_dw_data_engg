Locking table  edwpbs_staging.Stg_Rcom_Pars_CR for access
SELECT 'PBMCR350-10' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM edwpbs_staging.Stg_Rcom_Pars_CR ARP