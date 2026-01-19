export ODBC_EXP_DB='METRIQ'

export JOBNAME='J_MT_REF_NAT_SERV_CENTER_STG'
export AC_EXP_SQL_STATEMENT="Select 'J_MT_REF_NAT_SERV_CENTER_STG' + ','+ cast(coalesce(count(*),0) as varchar(20))+',' as SOURCE_STRING from [MDictionary].[dbo].[LookupCodes_NSC]"


export AC_ACT_SQL_STATEMENT="Select 'J_MT_REF_NAT_SERV_CENTER_STG'||','|| cast(coalesce(count(*),0) as varchar(20))||',' as SOURCE_STRING from ${NCR_STG_SCHEMA}.Ref_National_Svc_Center_Stg"




