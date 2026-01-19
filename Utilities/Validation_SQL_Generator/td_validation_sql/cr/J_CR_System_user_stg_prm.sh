export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_CR_System_user_stg'

export AC_EXP_SQL_STATEMENT="SELECT 'J_CR_System_user_stg'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING 
FROM  MDictionary.dbo.Security"

export AC_ACT_SQL_STATEMENT="select 'J_CR_System_user_stg'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_Staging.CR_System_User_Stg"


