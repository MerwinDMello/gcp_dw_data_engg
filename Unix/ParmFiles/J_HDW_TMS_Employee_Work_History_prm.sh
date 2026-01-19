##########################
## Variable Declaration ##
##########################

export Job_Name='J_HDW_TMS_Employee_Work_History'

export AC_EXP_SQL_STATEMENT="select '$JOBNAME' || ',' || cast(count(*) as varchar(20))  ||','||
coalesce(cast(SUM(Lawson_Company_Num) as VARCHAR(20)),'0') ||',' as SOURCE_STRING FROM $NCR_STG_SCHEMA.Employee_Work_History_Wrk"

export AC_ACT_SQL_STATEMENT="select '$JOBNAME' ||','|| cast(count(*) as varchar(20)) ||','||
coalesce(cast(SUM(Lawson_Company_Num) as VARCHAR(20)),'0')||',' as SOURCE_STRING FROM $NCR_TGT_SCHEMA.Employee_Work_History"
