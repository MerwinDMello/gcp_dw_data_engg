export JOBNAME='J_MT_REF_NAT_SERV_CENTER'
export AC_EXP_SQL_STATEMENT="SELECT 'J_MT_REF_NAT_SERV_CENTER'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM  $NCR_STG_SCHEMA.Ref_National_Svc_Center_Stg"

export AC_ACT_SQL_STATEMENT="select 'J_MT_REF_NAT_SERV_CENTER'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Ref_National_Service_Center  WHERE 
DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWCR_DMX_AC.ETL_JOB_RUN where Job_Name = 'J_MT_REF_NAT_SERV_CENTER');"
