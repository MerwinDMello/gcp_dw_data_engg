##########################
## Variable Declaration ##
##########################

export JOBNAME='J_HDW_TMS_Employee_Talent_Profile'
export Table_Name='Employee_Talent_Profile'
export AC_EXP_TOLERANCE_PERCENT=5

export AC_EXP_SQL_STATEMENT="
select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
/*FROM (
SELECT 
Employee_Talent_Profile_SID,
Employee_SID,
Employee_First_Name,
Employee_Middle_Name,
Employee_Last_name,
Employee_Num,
Employee_3_4_Login_Code,
Job_Family_Text,
Job_Code,
Job_Code_SID,
Position_Code_Text,
Position_Code,
Position_SID,
Position_Title_Text,
Org_Hierarchy_Text,
Manager_Name,
Manager_Employee_Num,
Travel_Willingness_Id,
Travel_Willingness_Pct_Range_Text,
Travel_Location_Text,
Relocation_Willingness_Id,
Relocation_Location_Text,
Calibrated_Performance_Rating_Id,
Overall_Performance_Rating_Id,
Employee_Promotability_Interest_Id,
Potential_Performance_Id,
Future_Role_1_Leadership_Level_Id,
Future_Role_1_Org_Size_Id,
Future_Role_1_Timeframe_Id,
Future_Role_1_Role_Id,
Future_Role_2_Leadership_Level_Id,
Future_Role_2_Org_Size_Id,
Future_Role_2_Timeframe_Id,
Future_Role_2_Role_Id,
Flight_Risk_Probability_Id,
Flight_Risk_Timeframe_Id,
Flight_Risk_Driver_Text,
Flight_Risk_Secondary_Driver_Text,
Lawson_Company_Num,
Process_Level_Code,
Source_System_Code,
DW_Last_Update_Date_Time

FROM EDWHR_Staging.Employee_Talent_Profile_Wrk WRK

WHERE 
(
Coalesce(Trim(WRK.Employee_Talent_Profile_SID), 0)
,Trim(Coalesce(WRK.Employee_SID, 0))
,TRIM(COALESCE(CAST(WRK.Employee_First_Name AS VARCHAR(40)),''))
,TRIM(COALESCE(CAST(WRK.Employee_Middle_Name AS VARCHAR(40)),''))
,TRIM(COALESCE(CAST(WRK.Employee_Last_name AS VARCHAR(40)),''))
,Trim(Coalesce(WRK.Employee_Num, 0))
,TRIM(COALESCE(CAST(WRK.Employee_3_4_Login_Code AS VARCHAR(7)),''))
,TRIM(COALESCE(CAST(WRK.Job_Family_Text AS VARCHAR(255)),''))
,TRIM(COALESCE(CAST(WRK.Job_Code AS VARCHAR(9)),''))
,Trim(Coalesce(WRK.Job_Code_SID, 0))
,TRIM(COALESCE(CAST(WRK.Position_Code_Text AS VARCHAR(255)),''))
,TRIM(COALESCE(CAST(WRK.Position_Code AS VARCHAR(255)),''))
,Trim(Coalesce(WRK.Position_SID, 0))
,TRIM(COALESCE(CAST(WRK.Position_Title_Text AS VARCHAR(255)),''))
,TRIM(COALESCE(CAST(WRK.Org_Hierarchy_Text AS VARCHAR(255)),''))
,TRIM(COALESCE(CAST(WRK.Manager_Name AS VARCHAR(80)),''))
,Trim(Coalesce(WRK.Manager_Employee_Num, 0))
,Trim(Coalesce(WRK.Travel_Willingness_Id, 0))
,TRIM(COALESCE(CAST(WRK.Travel_Willingness_Pct_Range_Text AS VARCHAR(8)),''))
,TRIM(COALESCE(CAST(WRK.Travel_Location_Text AS VARCHAR(8)),''))
,Trim(Coalesce(WRK.Relocation_Willingness_Id, 0))  
,TRIM(COALESCE(CAST(WRK.Relocation_Location_Text AS VARCHAR(8)),''))
,Trim(Coalesce(WRK.Calibrated_Performance_Rating_Id, 0) )
,Trim(Coalesce(WRK.Overall_Performance_Rating_Id, 0) )
,Trim(Coalesce(WRK.Employee_Promotability_Interest_Id, 0) )
,Trim(Coalesce(WRK.Potential_Performance_Id, 0) )
,Trim(Coalesce(WRK.Future_Role_1_Leadership_Level_Id, 0) )
,Trim(Coalesce(WRK.Future_Role_1_Org_Size_Id, 0) )
,Trim(Coalesce(WRK.Future_Role_1_Timeframe_Id, 0) )
,Trim(Coalesce(WRK.Future_Role_1_Role_Id, 0) )
,Trim(Coalesce(WRK.Future_Role_2_Leadership_Level_Id, 0) )
,Trim(Coalesce(WRK.Future_Role_2_Org_Size_Id, 0) )
,Trim(Coalesce(WRK.Future_Role_2_Timeframe_Id, 0) )
,Trim(Coalesce(WRK.Future_Role_2_Role_Id, 0) )
,Trim(Coalesce(WRK.Flight_Risk_Probability_Id, 0) )
,Trim(Coalesce(WRK.Flight_Risk_Timeframe_Id, 0) )
,TRIM(COALESCE(CAST(WRK.Flight_Risk_Driver_Text AS VARCHAR(8)),''))
,TRIM(COALESCE(CAST(WRK.Flight_Risk_Secondary_Driver_Text AS VARCHAR(8)),''))
,Trim(Coalesce(WRK.Lawson_Company_Num, 0) )
)
NOT IN 

(SELECT

Coalesce(Trim(TGT.Employee_Talent_Profile_SID), 0)
,Trim(Coalesce(TGT.Employee_SID, 0))
,TRIM(COALESCE(CAST(TGT.Employee_First_Name AS VARCHAR(40)),''))
,TRIM(COALESCE(CAST(TGT.Employee_Middle_Name AS VARCHAR(40)),''))
,TRIM(COALESCE(CAST(TGT.Employee_Last_name AS VARCHAR(40)),''))
,Trim(Coalesce(TGT.Employee_Num, 0))
,TRIM(COALESCE(CAST(TGT.Employee_3_4_Login_Code AS VARCHAR(7)),''))
,TRIM(COALESCE(CAST(TGT.Job_Family_Text AS VARCHAR(255)),''))
,TRIM(COALESCE(CAST(TGT.Job_Code AS VARCHAR(9)),''))
,Trim(Coalesce(TGT.Job_Code_SID, 0))
,TRIM(COALESCE(CAST(TGT.Position_Code_Text AS VARCHAR(255)),''))
,TRIM(COALESCE(CAST(TGT.Position_Code AS VARCHAR(255)),''))
,Trim(Coalesce(TGT.Position_SID, 0))
,TRIM(COALESCE(CAST(TGT.Position_Title_Text AS VARCHAR(255)),''))
,TRIM(COALESCE(CAST(TGT.Org_Hierarchy_Text AS VARCHAR(255)),''))
,TRIM(COALESCE(CAST(TGT.Manager_Name AS VARCHAR(80)),''))
,Trim(Coalesce(TGT.Manager_Employee_Num, 0))
,Trim(Coalesce(TGT.Travel_Willingness_Id, 0))
,TRIM(COALESCE(CAST(TGT.Travel_Willingness_Pct_Range_Text AS VARCHAR(8)),''))
,TRIM(COALESCE(CAST(TGT.Travel_Location_Text AS VARCHAR(8)),''))
,Trim(Coalesce(TGT.Relocation_Willingness_Id, 0))  
,TRIM(COALESCE(CAST(TGT.Relocation_Location_Text AS VARCHAR(8)),''))
,Trim(Coalesce(TGT.Calibrated_Performance_Rating_Id, 0) )
,Trim(Coalesce(TGT.Overall_Performance_Rating_Id, 0) )
,Trim(Coalesce(TGT.Employee_Promotability_Interest_Id, 0) )
,Trim(Coalesce(TGT.Potential_Performance_Id, 0) )
,Trim(Coalesce(TGT.Future_Role_1_Leadership_Level_Id, 0) )
,Trim(Coalesce(TGT.Future_Role_1_Org_Size_Id, 0) )
,Trim(Coalesce(TGT.Future_Role_1_Timeframe_Id, 0) )
,Trim(Coalesce(TGT.Future_Role_1_Role_Id, 0) )
,Trim(Coalesce(TGT.Future_Role_2_Leadership_Level_Id, 0) )
,Trim(Coalesce(TGT.Future_Role_2_Org_Size_Id, 0) )
,Trim(Coalesce(TGT.Future_Role_2_Timeframe_Id, 0) )
,Trim(Coalesce(TGT.Future_Role_2_Role_Id, 0) )
,Trim(Coalesce(TGT.Flight_Risk_Probability_Id, 0) )
,Trim(Coalesce(TGT.Flight_Risk_Timeframe_Id, 0) )
,TRIM(COALESCE(CAST(TGT.Flight_Risk_Driver_Text AS VARCHAR(8)),''))
,TRIM(COALESCE(CAST(TGT.Flight_Risk_Secondary_Driver_Text AS VARCHAR(8)),''))
,Trim(Coalesce(TGT.Lawson_Company_Num, 0) )


FROM EDWHR.Employee_Talent_Profile TGT WHERE valid_To_Date = '9999-12-31'
and DW_Last_Update_Date_Time < (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time FROM EDWHR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_HDW_TMS_Employee_Talent_Profile')
)
)a";*/

export AC_EXP_SQL_STATEMENT="
select '$Job_Name'||','||
cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM Edwhr_Staging.Employee_info STG
    where  DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time 
FROM EDWHR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_HDW_TMS_Employee_Info_Stg')
AND Substr(Trim(STG.Employee_ID), 1,1)  IN ('1','2','3','4','5','6','7','8','9')
AND   length(Trim(STG.Employee_ID)) <13 ";



export AC_ACT_SQL_STATEMENT="select 'J_HDW_TMS_Employee_Talent_Profile'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWHR.Employee_Talent_Profile
WHERE DW_Last_Update_Date_Time >= (SELECT MAX(Job_Start_Date_Time) as Job_Start_Date_Time 
FROM EDWHR_DMX_AC_BASE_VIEWS.ETL_JOB_RUN where Job_Name = 'J_HDW_TMS_Employee_Talent_Profile')";


