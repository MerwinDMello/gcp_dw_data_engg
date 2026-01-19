Locking table  edwpbs_staging.Stg_Rcom_Pars_CB for access
SELECT 'PBMCB340-10' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM edwpbs_staging.Stg_Rcom_Pars_CB ARP