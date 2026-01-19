
#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='PBMCB340-20'
export AC_ACT_SQL_STATEMENT="select 'PBMCB340-20' ||','|| cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS Source_String
from edwpbs.Fact_RCOM_PARS_Credit_Balance
where Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))"

export AC_EXP_SQL_STATEMENT="SELECT 'PBMCB340-20' || ',' || cast(zeroifnull(Count(1)) as varchar(20)) || ',' AS SOURCE_STRING
FROM edwpbs_staging.Stg_Rcom_Pars_CB ARP where Date_Sid = cast((add_months(current_date, -1) (format 'yyyymm')) as char(6))"




