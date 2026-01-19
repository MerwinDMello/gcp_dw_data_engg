#  @@START DMEXPRESS_EXPORTED_VARIABLES

export AC_EXP_TOLERANCE_PERCENT=0
export AC_EXP_TOLERANCE_AMT=0
export JOBNAME='J_IM_PCP_Users'

export ODBC_EXP_DB=$ODBC_PCP_DB
export ODBC_EXP_USER=$ODBC_PCP_USER
export ODBC_EXP_PASSWORD=$ODBC_PCP_PASSWORD

export AC_EXP_SQL_STATEMENT="select 'J_IM_PCP_Users' +','+cast(count(*) as varchar(20)) +',' AS SOURCE_STRING  
FROM
(

SELECT
	NPINumber
	,DivisionName
	,DivMnemonic
	,CreatedDate
	,Provider34ID
FROM	dbo.vw_providerenrollment_NPI_34
) A;" 

export AC_ACT_SQL_STATEMENT="select 'J_IM_PCP_Users'||','||cast(count(*) as varchar(20))||',' AS SOURCE_STRING from EDWIM_STAGING.PCP_Users;"


#  @@END DMEXPRESS_EXPORTED_VARIABLES
#  End of Exported variables section
#
