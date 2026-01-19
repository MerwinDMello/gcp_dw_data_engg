export JOBNAME='J_CN_PHYSICIAN_DETAIL'
export AC_EXP_SQL_STATEMENT="Select 'J_CN_PHYSICIAN_DETAIL'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from 
(
Select 
ROW_NUMBER() OVER( ORDER BY WRK.Physician_Name,WRK.Physician_Phone_Num)+(SEL COALESCE(MAX(Physician_ID), 900000) AS Physician_ID FROM $NCR_TGT_SCHEMA.CN_Physician_Detail) as Physician_ID,
WRK.Physician_Name,
WRK.Physician_Phone_Num,
'N' as Source_System_Code,
Current_Timestamp(0) as  DW_Last_Update_Date_Time

from $NCR_STG_SCHEMA.CN_Physician_Detail_STG_WRK WRK
Left outer join (Select Physician_Id,Physician_Name,Physician_Phone_Num from $NCR_TGT_SCHEMA.CN_Physician_Detail where Physician_Id>900000 ) TGT

on 

COALESCE(TRIM(WRK.Physician_Name),'XX')=COALESCE(TRIM(TGT.Physician_Name),'XX') and
COALESCE(TRIM(WRK.Physician_Phone_Num),'X')=COALESCE(TRIM(TGT.Physician_Phone_Num),'X')

where TGT.Physician_ID is NULL and
WRK.Physician_ID is NULL


UNION ALL

Select 
WRK.Physician_ID,
WRK.Physician_Name,
WRK.Physician_Phone_Num,
'N' as Source_System_Code,
Current_Timestamp(0) as  DW_Last_Update_Date_Time

from $NCR_STG_SCHEMA.CN_Physician_Detail_STG_WRK WRK

Left outer join (Select Physician_Id,Physician_Name,Physician_Phone_Num from $NCR_TGT_SCHEMA.CN_Physician_Detail where Physician_Id<=900000 ) TGT

on

COALESCE(TRIM(WRK.Physician_Name),'XX')=COALESCE(TRIM(TGT.Physician_Name),'XX') and
COALESCE(TRIM(WRK.Physician_Phone_Num),'X')=COALESCE(TRIM(TGT.Physician_Phone_Num),'X')

where TGT.Physician_ID is NULL and
WRK.Physician_ID is NOT NULL

) SRC"


export AC_ACT_SQL_STATEMENT="Select 'J_CN_PHYSICIAN_DETAIL'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING from ${NCR_TGT_SCHEMA}.CN_Physician_Detail
where DW_LAST_UPDATE_DATE_TIME >= (Select max(Job_Start_Date_time) from $NCR_AC_VIEW.ETL_JOB_RUN where Job_Name='J_CN_PHYSICIAN_DETAIL');"