
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMCB340-10'
export AC_ACT_SQL_STATEMENT="Locking table  edwpbs_staging.Stg_Rcom_Pars_CB for access
SELECT 'PBMCB340-10' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM edwpbs_staging.Stg_Rcom_Pars_CB ARP"

export AC_EXP_SQL_STATEMENT="Select 'PBMCB340-10' +','+ ltrim(rtrim(cast(count(1) as Varchar(20)))) +',' as Source_String from rcomCreditRefund.dbo.vw_expEDWEOM"




