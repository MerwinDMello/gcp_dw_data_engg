export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_MT_REF_LOOKUP_CODE_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_MT_REF_LOOKUP_CODE_STG' + ','+ cast(count(*) as varchar(20))+',' as SOURCE_STRING from mdictionary.dbo.LookupCodes_NSC"


export AC_ACT_SQL_STATEMENT="Select 'J_MT_REF_LOOKUP_CODE_STG'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.REF_LOOKUP_CODE_Stg"




