export JOBNAME='J_CN_PERSON_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PERSON_STG'+','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from DimPatient"
export AC_ACT_SQL_STATEMENT="Select 'J_CN_PERSON_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.CN_Person_STG"
