export Job_Name='J_IM_EDW_Staff_PM_Users'
export JOBNAME='J_IM_EDW_Staff_PM_Users'

export ODBC_EXP_DB=$ODBC_STAFFPM_DB
export ODBC_EXP_USER=$ODBC_STAFFPM_USER
export ODBC_EXP_PASSWORD=$ODBC_STAFFPM_PASSWORD

export AC_EXP_SQL_STATEMENT="select 'J_IM_EDW_Staff_PM_Users' +','+cast(COALESCE(A.counts,0) as varchar(20)) +',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM vw_EDWexclusions)A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_EDW_Staff_PM_Users' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
EDWIM_STAGING.Staff_PM_Users)A;"

