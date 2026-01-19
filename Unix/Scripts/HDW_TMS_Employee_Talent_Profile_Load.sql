#####################################################################################
#                                                                                   #
#   SCRIPT NAME     - HDW_Employee_Talent_Profile_Load.SQL                          #
#   Job NAME        - J_HDW_TMS_Employee_Talent_Profile                             #
#   TARGET TABLE    - EDWHR.Employee_Talent_Profile                                 #
#   Developer       - Julia Kim                                                     #
#   Editor          - Skylar 02/24/2020   - Changes as per HDM-1060                 #
#                                                                                   #
#   Version         - 1.0 - Initial RELEASE                                         #
#   Description     - The SCRIPT loads the TARGET TABLE WITH NEW records            #
#                     AND maintain their version AS TYPE 2                          #
#                                                                                   #
#####################################################################################


bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Employee_Talent_Profile;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/* Collecting Stats on look-up table */

/*CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_SK_Xwlk');*/
CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','Employee_Info','Trim(Employee_Id)', 'Employee_Talent_Profile');
CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR_STAGING','Employee_Info');


DELETE FROM edwhr_Staging.Employee_Info

where employee_id  in ('106431631', '119455195', '20949828', '43827771', '55558949');



DELETE FROM edwhr_staging.Employee_Talent_Profile_REJECT;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/* Inserting bad data from staging table into reject table*/


INSERT INTO edwhr_staging.Employee_Talent_Profile_REJECT
(
Login,
Employee_ID,
Last_Name,
First_Name,
Middle_Name,
Job_Family,
Job_Code,
Position_Title,
Organization_and_Hierarchy,
Manager,
Manager_Employee_ID,
Willing_To_Travel,
Willing_Travel_Percentage,
Employee_Mobilty_Preferences,
Willing_To_Relocate,
Relocation_Preferences,
Calibrate_Overall_Perf_Rating,
Overall_Performance_Rating,
Employee_Promote_Interest,
Potential,
Future_Role1_Leadership_Level,
Future_Role1_R_Function_Area,
Future_Role1_Org_Size_Scope,
Future_Role1_R_Timeframe,
Future_Role2_Leadership_Level,
Future_Role2_R_Function_Area,
Future_Role2_Org_Size_Scope,
Future_Role2_R_Timeframe,
Flight_Risk,
Flight_Risk_Timeframe,
External_Flight_Risk_Driver,
Secondary_Flight_Risk_Driver,
Jobs_Pooled_For_Count,
Positions_Talent_Pooled_Count,
Positions_Slated_For_Count,
Readiness_Unknown,
Readiness_Others,
Ready_Now,
Ready_6_11_Months,
Ready_12_18_Months,
Ready_18_24_Months,
Successors_Count,
Talent_Pool_Count,
Calibration_Session_Created_Date,
Calibration_Session_Name,
Calibration_Session_Last_Mod_Date_Time,
Calibration_Session_Published_Date,
Calibration_Session_Created_By_Name,
Company_Tenure_Text,
Calibration_Box_Name,
Position_Tenure_Text,
DW_Last_Update_Date_Time,
REJECT_REASON,
REJECT_STG_TBL_NM
)

SELECT

      STG.Login ,
      STG.Employee_ID ,
      STG.Last_Name ,
      STG.First_Name ,
      STG.Middle_Name ,
      STG.Job_Family ,
      STG.Job_Code ,
      STG.Position_Title ,
      STG.Organization_and_Hierarchy,
      STG.Manager ,
      STG.Manager_Employee_ID ,
      STG.Willing_To_Travel ,
      STG.Willing_Travel_Percentage ,
      STG.Employee_Mobilty_Preferences ,
      STG.Willing_To_Relocate ,
      STG.Relocation_Preferences ,
      STG.Calibrate_Overall_Perf_Rating ,
      STG.Overall_Performance_Rating ,
      STG.Employee_Promote_Interest ,
      STG.Potential ,
      STG.Future_Role1_Leadership_Level ,
      STG.Future_Role1_R_Function_Area ,
      STG. Future_Role1_Org_Size_Scope ,
      STG.Future_Role1_R_Timeframe ,
      STG.Future_Role2_Leadership_Level ,
      STG.Future_Role2_R_Function_Area ,
      STG.Future_Role2_Org_Size_Scope ,
      STG.Future_Role2_R_Timeframe ,
      STG.Flight_Risk ,
      STG.Flight_Risk_Timeframe ,
      STG.External_Flight_Risk_Driver ,
      STG.Secondary_Flight_Risk_Driver ,
      STG.Jobs_Pooled_For_Count ,
      STG.Positions_Talent_Pooled_Count ,
      STG.Positions_Slated_For_Count ,
      STG.Readiness_Unknown ,
      STG.Readiness_Others ,
      STG. Ready_Now ,
      STG.Ready_6_11_Months ,
      STG.Ready_12_18_Months ,
      STG.Ready_18_24_Months ,
      STG.Successors_Count ,
      STG.Talent_Pool_Count ,
	  cast(TRIM(STG.Calibrtn_Ses_Created_Dt) as date format 'mm/dd/yyyy') as Calibration_Session_Created_Date, 
	  STG.Calib_Sess_Name as Calibration_Session_Name,
	  CAST('0' || SUBSTR(TRIM(STG.Calibrtn_Ses_Last_Modif_Dt),0,6) || TRIM(extract(year from current_timestamp(0))) || ' ' || SUBSTR(TRIM(STG.Calibrtn_Ses_Last_Modif_Dt),9,8) || ' -05:00' as  timestamp(0) with time zone format 'mm/dd/yyyyBHH:MIBTBZ')  as Calibration_Session_Last_Mod_Date_Time,
	  case when TRIM(STG.Clb_Ses_Publ_to_Talnt_Prfl_Dt) = '' then null else cast(TRIM(STG.Clb_Ses_Publ_to_Talnt_Prfl_Dt) as date) end as Clb_Ses_Publ_to_Talnt_Prfl_Dt,
	  STG.Calibrtn_Ses_Created_By as Calibration_Session_Created_By_Name,
	  STG.Time_With_Company as Company_Tenure_Text,
	  STG.Calibration_Box_Name as Calibration_Box_Name,
	  STG.Time_In_Position as Position_Tenure_Text,
      CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,
      (CASE WHEN Trim(STG.Employee_ID) IS  NULL
	    THEN 'Employee_ID is NULL'
	    WHEN Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    THEN  'Employee_Id is alpha_numeric'
    	    ELSE 0 END )
                      AS REJECT_REASON,
      'Employee_Info' AS REJECT_STG_TBL_NM
      FROM Edwhr_Staging.Employee_info STG
     Where Substr(Trim(STG.Employee_ID), 1,1) NOT IN ('1','2','3','4','5','6','7','8','9')
     OR length(Trim(STG.Employee_ID)) >= 13 
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


DELETE FROM EDWHR_STAGING.Employee_Talent_Profile_Wrk;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Load Work Table with working Data */

INSERT INTO EDWHR_Staging.Employee_Talent_Profile_Wrk
(
Employee_Talent_Profile_SID,
Employee_SID,
Employee_First_Name,
Employee_Middle_Name,
Employee_Last_name,
Employee_Num,
Employee_3_4_Login_Code,
Job_Family_Text,
str1,
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
Calibration_Session_Created_Date,
Calibration_Session_Name,
Calibration_Session_Last_Mod_Date_Time,
Calibration_Session_Published_Date,
Calibration_Session_Created_By_Name,
Calibration_Box_Id,
Company_Tenure_Text,
Position_Tenure_Text,
Lawson_Company_Num,
Process_Level_Code,
Last_Pub_Calibration_Session,
Last_Pub_Calibration_Box_Rank,
Source_System_Code,
DW_Last_Update_Date_Time
)

sel iq.* from
(
SELECT 

(CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
      THEN XWLK.SK ELSE 0 END )
                      AS Employee_Talent_Profile_SID,
Coalesce(Trim(EMP.Employee_Sid), 0) AS Employee_SID,
STG.First_Name AS Employee_First_Name,
STG.Middle_Name AS Employee_Middle_Name,
STG.Last_Name AS Employee_Last_name,
CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
          Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9') 
     THEN Coalesce(Trim(STG.Employee_ID), 0)
     ELSE 0 END AS Employee_Num,
EMP1.Employee_34_Login_Code AS Employee_3_4_Login_Code,

STG.Job_Family AS Job_Family_Text,

CASE WHEN POSITION('-' in STG.Job_Family) <> 0
     THEN SUBSTR(STG.Job_Family,1,POSITION('-' in STG.Job_Family)-1) ELSE NULL END AS str1,
 
CASE WHEN CHAR_LENGTH(str1) >=  9 THEN SUBSTR(str1,5,5) 
     WHEN CHAR_LENGTH(str1) = 5 THEN SUBSTR(str1,1,5)
     ELSE NULL END AS J_Code,

Trim(Coalesce(JBC.Job_Code_SID,0)) AS Job_Code_SID,
STG.Job_Code AS Position_Code_Text,
CASE WHEN (char_length(STG.Job_Code) -4) > 0 
     THEN Substr(STG.Job_Code, 5, (char_length(STG.Job_Code) -4)) 
     ELSE  NULL END  AS Position_Code,
Trim(Coalesce(JBP.Position_Sid,0)) AS Position_SID,
--JBP.Position_Sid AS Position_Sid,
STG.Position_Title   AS Position_Title_Text,
STG.Organization_and_Hierarchy AS Org_Hierarchy_Text,
STG.Manager AS Manager_Name,
CASE WHEN Trim(STG.Manager_Employee_ID) IS NOT NULL  AND
          Substr(Trim(STG.Manager_Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9') 
     THEN Coalesce(Trim(STG.Manager_Employee_ID), 0) 
     ELSE 0 END AS Manager_Employee_Num,
Trim(Coalesce(RLW.Location_Willingness_Id, 0)) AS Travel_Willingness_Id,
STG.Willing_Travel_Percentage AS Travel_Willingness_Pct_Range_Text,
STG.Employee_Mobilty_Preferences AS Travel_Location_Text,
Trim(RLW2.Location_Willingness_Id) AS Relocation_Willingness_Id,
STG.Relocation_Preferences AS Relocation_Location_Text,
Trim(Coalesce(RPR.Performance_Rating_Id, 0)) AS Calibrated_Performance_Rating_Id,
Trim(Coalesce(RPR2.Performance_Rating_Id, 0)) AS Overall_Performance_Rating_Id,
Trim(Coalesce(RPP.Probability_Potential_Id, 0))AS Employee_Promotability_Interest_Id,
Trim(Coalesce(RPP2.Probability_Potential_Id, 0)) AS Potential_Performance_Id,
Trim(Coalesce(RFR.Future_Role_Attribute_Id,0 ))AS Future_Role_1_Leadership_Level_Id,
Trim(Coalesce(RFR2.Future_Role_Attribute_Id,0)) AS Future_Role_1_Org_Size_Id,
Trim(Coalesce(RFR3.Future_Role_Attribute_Id,0)) AS Future_Role_1_Timeframe_Id,
Trim(Coalesce(RFR4.Future_Role_Attribute_Id,0)) AS Future_Role_1_Role_Id,
Trim(Coalesce(RFR5.Future_Role_Attribute_Id,0)) AS Future_Role_2_Leadership_Level_Id,
Trim(Coalesce(RFR6.Future_Role_Attribute_Id,0)) AS Future_Role_2_Org_Size_Id,
Trim(Coalesce(RFR7.Future_Role_Attribute_Id,0)) AS Future_Role_2_Timeframe_Id,
Trim(Coalesce(RFR8.Future_Role_Attribute_Id,0)) AS Future_Role_2_Role_Id,
Trim(Coalesce(RPP3.Probability_Potential_Id,0)) AS Flight_Risk_Probability_Id,
Trim(Coalesce(RT.Timeframe_Id,0)) AS Flight_Risk_Timeframe_Id,
STG.External_Flight_Risk_Driver AS Flight_Risk_Driver_Text,
STG.Secondary_Flight_Risk_Driver AS Flight_Risk_Secondary_Driver_Text,
cast(TRIM(STG.Calibrtn_Ses_Created_Dt) as date format 'mm/dd/yyyy') AS Calibration_Session_Created_Date, 
STG.Calib_Sess_Name AS Calibration_Session_Name,
cast(
CASE WHEN Length(SUBSTR(Trim(Calibrtn_Ses_Last_Modif_Dt),1,InStr(Calibrtn_Ses_Last_Modif_Dt,'/')-1)) <2 Then '0'||SUBSTR(Trim(Calibrtn_Ses_Last_Modif_Dt),1,InStr(Calibrtn_Ses_Last_Modif_Dt,'/')-1)||'/'
ELSE SUBSTR(Trim(Calibrtn_Ses_Last_Modif_Dt),1,InStr(Calibrtn_Ses_Last_Modif_Dt,'/')-1)||'/' END||CASE WHEN Length (OREPLACE(SUBSTR(Trim(Calibrtn_Ses_Last_Modif_Dt),INSTR (Calibrtn_Ses_Last_Modif_Dt,'/')+1,2),'/',Null)) < 2 Then '0'||OREPLACE(SUBSTR(Trim(Calibrtn_Ses_Last_Modif_Dt),INSTR (Calibrtn_Ses_Last_Modif_Dt,'/')+1,2),'/',Null)||'/'
ELSE OREPLACE(SUBSTR(Trim(Calibrtn_Ses_Last_Modif_Dt),INSTR (Calibrtn_Ses_Last_Modif_Dt,'/')+1,2),'/',Null)||'/' END|| TRIM(extract(year from current_timestamp(0))) 
|| ' ' ||Case When Length(Trim(SUBSTR(Calibrtn_Ses_Last_Modif_Dt,INSTR(Calibrtn_Ses_Last_Modif_Dt,' ')+1,8))) = 7 Then '0'||Trim(SUBSTR(Calibrtn_Ses_Last_Modif_Dt,INSTR(Calibrtn_Ses_Last_Modif_Dt,' ')+1,8))
ELSE Trim(SUBSTR(Calibrtn_Ses_Last_Modif_Dt,INSTR(Calibrtn_Ses_Last_Modif_Dt,' ')+1,8)) END || ' -05:00' as  timestamp(0) with time zone format 'mm/dd/yyyyBHH:MIBTBZ') AS Calibration_Session_Last_Mod_Date_Time ,
case when TRIM(STG.Clb_Ses_Publ_to_Talnt_Prfl_Dt) = '' then null else cast(TRIM(STG.Clb_Ses_Publ_to_Talnt_Prfl_Dt) as date  format 'MM/DD/YYYY') end as Clb_Ses_Publ_to_Talnt_Prfl_Dt,
STG.Calibrtn_Ses_Created_By as Calibration_Session_Created_By_Name,
Case when  Trim(Calibration_Box_Name) = 'Not Specified' Then 100
	 when  Trim(Calibration_Box_Name) = 'Too Soon To Tell' Then 101
Else Cast(Substring(STG.Calibration_Box_Name,1,1) as Integer) END AS Calibration_Box_Id,
STG.Time_With_Company AS Company_Tenure_Text,
STG.Time_In_Position AS Position_Tenure_Text,
Coalesce(CAST(Substr(STG.Job_Code,1,4) AS INT), 0) AS Lawson_Company_Num,
'00000' AS Process_Level_Code,
STG.Last_Pub_Calibration_Session AS Last_Pub_Calibration_Session,
STG.Last_Pub_Calibration_Box_Rank AS Last_Pub_Calibration_Box_Rank,
'M' AS Source_System_Code,
Current_Timestamp(0) AS DW_Last_Update_Date_Time
FROM  EDWHR_STAGING.Employee_Info STG                                                                

INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
ON trim(stg.Employee_Id)  = XWLK.SK_Source_Txt 
AND XWLK.SK_Type = 'Employee_Talent_Profile'

                                                             
LEFT JOIN EDWHR_BASE_VIEWS.Employee EMP
ON COALESCE(TRIM(STG.Employee_id),0) = COALESCE(TRIM(EMP.Employee_Num),0)
AND Cast(Substr(STG.Job_Code, 1,4) AS Integer) = EMP.Lawson_Company_Num 
AND EMP.Valid_To_Date = '9999-12-31'

LEFT JOIN EDWHR_BASE_VIEWS.Employee EMP1
ON COALESCE(TRIM(STG.Employee_id),0) = COALESCE(TRIM(EMP1.Employee_Num),0)
AND EMP1.Valid_To_Date = '9999-12-31'


LEFT JOIN EDWHR_BASE_VIEWS.Job_Code JBC
ON J_code = JBC.job_code
AND Cast(Substr(STG.Job_Code, 1,4) AS INT) = JBC.lawson_company_num
AND JBC.Valid_To_Date = '9999-12-31'

LEFT JOIN (sel distinct Lawson_Company_Num, Position_Code, Position_Sid, Valid_To_Date from EDWHR_BASE_VIEWS.Job_Position) JBP
ON CASE WHEN (char_length(STG.Job_Code) -4) > 0 
	THEN Substr(STG.Job_Code, 5, (char_length(STG.Job_Code)-4)) END= JBP.Position_Code
AND Cast(Substr(STG.Job_Code, 1,4) AS INT) =JBP.Lawson_Company_Num 
AND JBP.Valid_To_Date ='9999-12-31'


/*LEFT JOIN (sel distinct Lawson_Company_Num, Position_Code, Position_Sid, Valid_To_Date from EDWHR_BASE_VIEWS.Job_Position) JBP
ON  (CASE WHEN position(Substr(STG.Job_Family,1,9) in STG.Job_Family) = 9   THEN Substr(STG.Job_Family,5,5) 
	  WHEN position(Substr(STG.Job_Family,1,5) in STG.Job_Family) =5 Then Substr(STG.Job_Family,1,5) 
	  ELSE  Substr(STG.Job_Family,1,5) 
	  END) = STG.Job_Code
AND Cast(Substr(STG.Job_Code, 1,4) AS INT) =JBP.Lawson_Company_Num 
AND CASE WHEN (char_length(STG.Job_Code) -4) > 0 
         THEN Substr(STG.Job_Code, 5, (char_length(STG.Job_Code) -4))  ELSE  NULL END  = JBP.Position_Code 
AND JBP.Valid_To_Date ='9999-12-31'*/


LEFT JOIN EDWHR_BASE_VIEWS.Ref_Location_Willingness RLW
ON  RLW.Location_Willingness_Desc = STG.Willing_To_Travel

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Location_Willingness RLW2
ON  RLW2.Location_Willingness_Desc = STG.Willing_To_Relocate


LEFT JOIN EDWHR_BASE_VIEWS.REF_Performance_Rating RPR
ON RPR.Performance_Rating_Desc =STG.Calibrate_Overall_Perf_Rating

LEFT JOIN EDWHR_BASE_VIEWS.REF_Performance_Rating RPR2
ON RPR2.Performance_Rating_Desc =STG.Overall_Performance_Rating

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Probability_Potential RPP
ON RPP.Probability_Potential_Desc =STG.Employee_Promote_Interest

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Probability_Potential RPP2
ON RPP2.Probability_Potential_Desc = STG.Potential

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Future_Role_Attribute RFR
ON RFR.Future_Role_Attribute_Desc = STG.Future_Role1_Leadership_Level 

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Future_Role_Attribute RFR2
ON RFR2.Future_Role_Attribute_Desc = STG.Future_Role1_Org_Size_Scope

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Future_Role_Attribute RFR3
ON RFR3.Future_Role_Attribute_Desc = STG.Future_Role1_R_Timeframe

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Future_Role_Attribute RFR4
ON RFR4.Future_Role_Attribute_Desc = STG.Future_Role1_R_Function_Area

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Future_Role_Attribute RFR5
ON RFR5.Future_Role_Attribute_Desc = STG.Future_Role2_Leadership_Level

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Future_Role_Attribute RFR6
ON RFR6.Future_Role_Attribute_Desc = STG.Future_Role2_Org_Size_Scope


LEFT JOIN EDWHR_BASE_VIEWS.Ref_Future_Role_Attribute RFR7
ON RFR7.Future_Role_Attribute_Desc = STG.Future_Role2_R_Timeframe

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Future_Role_Attribute RFR8
ON RFR8.Future_Role_Attribute_Desc = STG.Future_Role2_R_Function_Area

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Probability_Potential RPP3
ON RPP3.Probability_Potential_Desc =STG.Flight_Risk

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Timeframe RT
ON Trim(RT.Timeframe_Desc) = Trim(STG.Flight_Risk_Timeframe)

where length(Trim(STG.Employee_ID)) < 13
And Substr(Trim(STG.Employee_ID), 1,1) IN ('1','2','3','4','5','6','7','8','9') 
and TRYCAST(STG.Calibrtn_Ses_Created_Dt as date) is null
) iq
where iq.Last_Pub_Calibration_Session=iq.Calibration_Session_Name
qualify Row_Number () Over (Partition by Employee_Num Order by Calibration_Session_Last_Mod_Date_Time desc)  = 1
;


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/* Collecting Stats on work table/s */

CALL dbadmin_procs.collect_stats_table ('EDWHR_STAGING','Employee_Talent_Profile_Wrk');

BT;



UPDATE TGT
FROM EDWHR.Employee_Talent_Profile TGT,
EDWHR_Staging.Employee_Talent_Profile_Wrk WRK
/*(select * from  EDWHR_Staging.Employee_Talent_Profile_Wrk
where Last_Pub_Calibration_Session = Calibration_Session_Name
qualify Row_Number () Over (Partition by Employee_Num Order by Calibration_Session_Last_Mod_Date_Time desc)  = 1) WRK */
SET  Valid_To_Date = Current_Date  - INTERVAL '1' DAY,
			 DW_Last_Update_Date_Time = Current_Timestamp(0)
WHERE 
			TGT.Employee_Talent_Profile_SID = WRK.Employee_Talent_Profile_SID
AND (
			Coalesce(Trim(TGT.Employee_SID), 0) <>Coalesce(Trim(WRK.Employee_SID), 0) 
			OR TRIM(COALESCE(CAST(TGT.Employee_First_Name AS VARCHAR(40)),'')) <> TRIM(COALESCE(CAST(WRK.Employee_First_Name AS VARCHAR(40)),''))
			OR TRIM(COALESCE(CAST(TGT.Employee_Middle_Name AS VARCHAR(40)),'')) <> TRIM(COALESCE(CAST(WRK.Employee_Middle_Name AS VARCHAR(40)),''))
			OR TRIM(COALESCE(CAST(TGT.Employee_Last_name AS VARCHAR(40)),'')) <> TRIM(COALESCE(CAST(WRK.Employee_Last_name AS VARCHAR(40)),''))
			OR Coalesce(Trim(TGT.Employee_Num), 0) <>Coalesce(Trim(WRK.Employee_Num), 0) 
			OR TRIM(COALESCE(CAST(TGT.Employee_3_4_Login_Code AS VARCHAR(7)),'')) <> TRIM(COALESCE(CAST(WRK.Employee_3_4_Login_Code AS VARCHAR(7)),''))
			OR TRIM(COALESCE(CAST(TGT.Job_Family_Text AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Job_Family_Text AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Job_Code AS VARCHAR(9)),'')) <> TRIM(COALESCE(CAST(WRK.Job_Code AS VARCHAR(9)),''))
			OR Coalesce(Trim(TGT.Job_Code_SID), 0) <>Coalesce(Trim(WRK.Job_Code_SID), 0) 
			OR TRIM(COALESCE(CAST(TGT.Position_Code_Text AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Position_Code_Text AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Position_Code AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Position_Code AS VARCHAR(255)),''))
			OR Coalesce(Trim(TGT.Position_SID), 0) <>Coalesce(Trim(WRK.Position_SID), 0) 
			OR TRIM(COALESCE(CAST(TGT.Position_Title_Text AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Position_Title_Text AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Org_Hierarchy_Text AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Org_Hierarchy_Text AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Manager_Name AS VARCHAR(80)),'')) <> TRIM(COALESCE(CAST(WRK.Manager_Name AS VARCHAR(80)),''))
			OR Coalesce(Trim(TGT.Manager_Employee_Num), 0) <>Coalesce(Trim(WRK.Manager_Employee_Num), 0) 
			OR Coalesce(Trim(TGT.Travel_Willingness_Id), 0) <>Coalesce(Trim(WRK.Travel_Willingness_Id), 0) 
			OR TRIM(COALESCE(CAST(TGT.Travel_Willingness_Pct_Range_Text AS VARCHAR(8)),'')) <> TRIM(COALESCE(CAST(WRK.Travel_Willingness_Pct_Range_Text AS VARCHAR(8)),''))
			OR TRIM(COALESCE(CAST(TGT.Travel_Location_Text AS VARCHAR(100)),'')) <> TRIM(COALESCE(CAST(WRK.Travel_Location_Text AS VARCHAR(100)),''))
			OR Trim(Coalesce(TGT.Relocation_Willingness_Id, 0))  <>Trim(Coalesce(WRK.Relocation_Willingness_Id, 0))  
			OR TRIM(COALESCE(CAST(TGT.Relocation_Location_Text AS VARCHAR(500)),'')) <> TRIM(COALESCE(CAST(WRK.Relocation_Location_Text AS VARCHAR(500)),''))
			OR Coalesce(Trim(TGT.Calibrated_Performance_Rating_Id), 0) <>Coalesce(Trim(WRK.Calibrated_Performance_Rating_Id), 0)
			OR Coalesce(Trim(TGT.Overall_Performance_Rating_Id), 0) <>Coalesce(Trim(WRK.Overall_Performance_Rating_Id), 0) 
			OR Coalesce(Trim(TGT.Employee_Promotability_Interest_Id), 0) <>Coalesce(Trim(WRK.Employee_Promotability_Interest_Id), 0) 
			OR Coalesce(Trim(TGT.Potential_Performance_Id), 0) <>Coalesce(Trim(WRK.Potential_Performance_Id), 0) 
			OR Coalesce(Trim(TGT.Future_Role_1_Leadership_Level_Id), 0) <>Coalesce(Trim(WRK.Future_Role_1_Leadership_Level_Id), 0) 
			OR Coalesce(Trim(TGT.Future_Role_1_Org_Size_Id), 0) <>Coalesce(Trim(WRK.Future_Role_1_Org_Size_Id), 0) 
			OR Coalesce(Trim(TGT.Future_Role_1_Timeframe_Id), 0) <>Coalesce(Trim(WRK.Future_Role_1_Timeframe_Id), 0) 
			OR Coalesce(Trim(TGT.Future_Role_1_Role_Id), 0) <>Coalesce(Trim(WRK.Future_Role_1_Role_Id), 0) 
			OR Coalesce(Trim(TGT.Future_Role_2_Leadership_Level_Id), 0) <>Coalesce(Trim(WRK.Future_Role_2_Leadership_Level_Id), 0) 
			OR Coalesce(Trim(TGT.Future_Role_2_Org_Size_Id), 0) <>Coalesce(Trim(WRK.Future_Role_2_Org_Size_Id), 0)
			OR Coalesce(Trim(TGT.Future_Role_2_Timeframe_Id), 0) <>Coalesce(Trim(WRK.Future_Role_2_Timeframe_Id), 0) 
			OR Coalesce(Trim(TGT.Future_Role_2_Role_Id), 0) <>Coalesce(Trim(WRK.Future_Role_2_Role_Id), 0) 
			OR Coalesce(Trim(TGT.Flight_Risk_Probability_Id), 0) <>Coalesce(Trim(WRK.Flight_Risk_Probability_Id), 0) 
			OR Coalesce(Trim(TGT.Flight_Risk_Timeframe_Id), 0) <>Coalesce(Trim(WRK.Flight_Risk_Timeframe_Id), 0) 
			OR TRIM(COALESCE(CAST(TGT.Flight_Risk_Driver_Text AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Flight_Risk_Driver_Text AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Flight_Risk_Secondary_Driver_Text AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Flight_Risk_Secondary_Driver_Text AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Calibration_Session_Created_Date AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Calibration_Session_Created_Date AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Calibration_Session_Name AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Calibration_Session_Name AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Calibration_Session_Last_Mod_Date_Time AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Calibration_Session_Last_Mod_Date_Time AS VARCHAR(255)),''))
            OR TRIM(COALESCE(CAST(TGT.Calibration_Session_Published_Date AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Calibration_Session_Published_Date AS VARCHAR(255)),''))
            OR TRIM(COALESCE(CAST(TGT.Calibration_Session_Created_By_Name AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Calibration_Session_Created_By_Name AS VARCHAR(255)),''))
			OR Coalesce(Trim(TGT.Calibration_Box_Id), 0) <>Coalesce(Trim(WRK.Calibration_Box_Id), 0)
			OR TRIM(COALESCE(CAST(TGT.Company_Tenure_Text AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Company_Tenure_Text AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Position_Tenure_Text AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Position_Tenure_Text AS VARCHAR(255)),''))
			OR Coalesce(Trim(TGT.Lawson_Company_Num), 0) <>Coalesce(Trim(WRK.Lawson_Company_Num), 0) 
			)
AND TGT.Valid_TO_Date = DATE '9999-12-31'

;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


INSERT INTO EDWHR.Employee_Talent_Profile
(
Employee_Talent_Profile_SID,
Valid_From_Date,
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
Valid_To_Date,
Calibration_Session_Created_Date,
Calibration_Session_Name,
Calibration_Session_Last_Mod_Date_Time,
Calibration_Session_Published_Date,
Calibration_Session_Created_By_Name,
Calibration_Box_Id,
Company_Tenure_Text,
Position_Tenure_Text,
Lawson_Company_Num,
Process_Level_Code,
Source_System_Code,
DW_Last_Update_Date_Time
)
select
Coalesce(Trim(WRK.Employee_Talent_Profile_SID), 0),
Current_Date AS Valid_From_Date,
Coalesce(Trim(WRK.Employee_SID), 0),
TRIM(COALESCE(CAST(WRK.Employee_First_Name AS VARCHAR(40)),'')),
TRIM(COALESCE(CAST(WRK.Employee_Middle_Name AS VARCHAR(40)),'')),
TRIM(COALESCE(CAST(WRK.Employee_Last_name AS VARCHAR(40)),'')),
Coalesce(Trim(WRK.Employee_Num), 0),
TRIM(COALESCE(CAST(WRK.Employee_3_4_Login_Code AS VARCHAR(7)),'')),
TRIM(COALESCE(CAST(WRK.Job_Family_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Job_Code AS VARCHAR(9)),'')),
Coalesce(Trim(WRK.Job_Code_SID), 0) ,
TRIM(COALESCE(CAST(WRK.Position_Code_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Position_Code AS VARCHAR(255)),'')),
Coalesce(Trim(WRK.Position_SID), 0) ,
TRIM(COALESCE(CAST(WRK.Position_Title_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Org_Hierarchy_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Manager_Name AS VARCHAR(80)),'')),
Coalesce(Trim(WRK.Manager_Employee_Num), 0) ,
Coalesce(Trim(WRK.Travel_Willingness_Id), 0) ,
TRIM(COALESCE(CAST(WRK.Travel_Willingness_Pct_Range_Text AS VARCHAR(8)),'')),
TRIM(COALESCE(CAST(WRK.Travel_Location_Text AS VARCHAR(100)),'')),
Trim(Coalesce(WRK.Relocation_Willingness_Id, 0)),
TRIM(COALESCE(CAST(WRK.Relocation_Location_Text AS VARCHAR(500)),'')),
Coalesce(Trim(WRK.Calibrated_Performance_Rating_Id), 0),
Coalesce(Trim(WRK.Overall_Performance_Rating_Id), 0) ,
Coalesce(Trim(WRK.Employee_Promotability_Interest_Id), 0),
Coalesce(Trim(WRK.Potential_Performance_Id), 0),
Coalesce(Trim(WRK.Future_Role_1_Leadership_Level_Id), 0),
Coalesce(Trim(WRK.Future_Role_1_Org_Size_Id), 0),
Coalesce(Trim(WRK.Future_Role_1_Timeframe_Id), 0) ,
Coalesce(Trim(WRK.Future_Role_1_Role_Id), 0) ,
Coalesce(Trim(WRK.Future_Role_2_Leadership_Level_Id), 0),
Coalesce(Trim(WRK.Future_Role_2_Org_Size_Id), 0),
Coalesce(Trim(WRK.Future_Role_2_Timeframe_Id), 0),
Coalesce(Trim(WRK.Future_Role_2_Role_Id), 0) ,
Coalesce(Trim(WRK.Flight_Risk_Probability_Id), 0) ,
Trim(Coalesce(WRK.Flight_Risk_Timeframe_Id, 0)),
TRIM(COALESCE(CAST(WRK.Flight_Risk_Driver_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Flight_Risk_Secondary_Driver_Text AS VARCHAR(255)),'')),
'9999-12-31' AS Valid_To_Date,
Calibration_Session_Created_Date,
Calibration_Session_Name,
Calibration_Session_Last_Mod_Date_Time,
Calibration_Session_Published_Date,
Calibration_Session_Created_By_Name,
Coalesce(Trim(WRK.Calibration_Box_Id), 0),
Company_Tenure_Text,
Position_Tenure_Text,
Coalesce(Trim(WRK.Lawson_Company_Num), 0),
Process_Level_Code,
Source_System_Code,
Current_Timestamp(0) As DW_Last_Update_Date_Time
from EDWHR_Staging.Employee_Talent_Profile_Wrk WRK
WHERE
(
WRK.Employee_Talent_Profile_SID, 
Coalesce(Trim(WRK.Employee_SID), 0),
TRIM(COALESCE(CAST(WRK.Employee_First_Name AS VARCHAR(40)),'')),
TRIM(COALESCE(CAST(WRK.Employee_Middle_Name AS VARCHAR(40)),'')),
TRIM(COALESCE(CAST(WRK.Employee_Last_name AS VARCHAR(40)),'')),
Coalesce(Trim(WRK.Employee_Num), 0),
TRIM(COALESCE(CAST(WRK.Employee_3_4_Login_Code AS VARCHAR(7)),'')),
TRIM(COALESCE(CAST(WRK.Job_Family_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Job_Code AS VARCHAR(9)),'')),
Coalesce(Trim(WRK.Job_Code_SID), 0) ,
TRIM(COALESCE(CAST(WRK.Position_Code_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Position_Code AS VARCHAR(255)),'')),
Coalesce(Trim(WRK.Position_SID), 0) ,
TRIM(COALESCE(CAST(WRK.Position_Title_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Org_Hierarchy_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Manager_Name AS VARCHAR(80)),'')),
Coalesce(Trim(WRK.Manager_Employee_Num), 0) ,
Coalesce(Trim(WRK.Travel_Willingness_Id), 0) ,
TRIM(COALESCE(CAST(WRK.Travel_Willingness_Pct_Range_Text AS VARCHAR(8)),'')),
TRIM(COALESCE(CAST(WRK.Travel_Location_Text AS VARCHAR(100)),'')),
Trim(Coalesce(WRK.Relocation_Willingness_Id, 0)),
TRIM(COALESCE(CAST(WRK.Relocation_Location_Text AS VARCHAR(500)),'')),
Coalesce(Trim(WRK.Calibrated_Performance_Rating_Id), 0),
Coalesce(Trim(WRK.Overall_Performance_Rating_Id), 0) ,
Coalesce(Trim(WRK.Employee_Promotability_Interest_Id), 0),
Coalesce(Trim(WRK.Potential_Performance_Id), 0),
Coalesce(Trim(WRK.Future_Role_1_Leadership_Level_Id), 0),
Coalesce(Trim(WRK.Future_Role_1_Org_Size_Id), 0),
Coalesce(Trim(WRK.Future_Role_1_Timeframe_Id), 0) ,
Coalesce(Trim(WRK.Future_Role_1_Role_Id), 0) ,
Coalesce(Trim(WRK.Future_Role_2_Leadership_Level_Id), 0),
Coalesce(Trim(WRK.Future_Role_2_Org_Size_Id), 0),
Trim(Coalesce(WRK.Future_Role_2_Timeframe_Id, 0)),
Coalesce(Trim(WRK.Future_Role_2_Role_Id), 0) ,
Coalesce(Trim(WRK.Flight_Risk_Probability_Id), 0) ,
Coalesce(Trim(WRK.Flight_Risk_Timeframe_Id), 0),
TRIM(COALESCE(CAST(WRK.Flight_Risk_Driver_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(WRK.Flight_Risk_Secondary_Driver_Text AS VARCHAR(255)),'')),
TRIM(COALESCE(CAST(Calibration_Session_Created_Date as VARCHAR(255)),'')),
TRIM(COALESCE(Calibration_Session_Name, '' )),
TRIM(COALESCE(CAST(Calibration_Session_Last_Mod_Date_Time as VARCHAR(255)),'')),
TRIM(COALESCE(CAST(Calibration_Session_Published_Date as VARCHAR(255)),'')),
TRIM(COALESCE(Calibration_Session_Created_By_Name, '')),
Coalesce(Trim(WRK.Calibration_Box_Id), 0),
TRIM(COALESCE(Company_Tenure_Text, '')),
TRIM(COALESCE(Position_Tenure_Text, '')),
Coalesce(Trim(WRK.Lawson_Company_Num), 0) 
)
NOT IN 
(SELECT 
			TGT.Employee_Talent_Profile_SID,
			 Coalesce(Trim(TGT.Employee_SID), 0) ,
			 TRIM(COALESCE(CAST(TGT.Employee_First_Name AS VARCHAR(40)),'')) ,
			 TRIM(COALESCE(CAST(TGT.Employee_Middle_Name AS VARCHAR(40)),'')) ,
			 TRIM(COALESCE(CAST(TGT.Employee_Last_name AS VARCHAR(40)),'')) ,
			 Coalesce(Trim(TGT.Employee_Num), 0) ,
			 TRIM(COALESCE(CAST(TGT.Employee_3_4_Login_Code AS VARCHAR(7)),'')) ,
			 TRIM(COALESCE(CAST(TGT.Job_Family_Text AS VARCHAR(255)),'')) ,
			 TRIM(COALESCE(CAST(TGT.Job_Code AS VARCHAR(9)),'')) ,
			 Coalesce(Trim(TGT.Job_Code_SID), 0),
			 TRIM(COALESCE(CAST(TGT.Position_Code_Text AS VARCHAR(255)),'')) ,
			 TRIM(COALESCE(CAST(TGT.Position_Code AS VARCHAR(255)),'')) ,
			 Coalesce(Trim(TGT.Position_SID), 0),
			 TRIM(COALESCE(CAST(TGT.Position_Title_Text AS VARCHAR(255)),'')),
			 TRIM(COALESCE(CAST(TGT.Org_Hierarchy_Text AS VARCHAR(255)),'')) ,
			 TRIM(COALESCE(CAST(TGT.Manager_Name AS VARCHAR(80)),'')),
			 Coalesce(Trim(TGT.Manager_Employee_Num), 0) ,
			 Coalesce(Trim(TGT.Travel_Willingness_Id), 0),
			 TRIM(COALESCE(CAST(TGT.Travel_Willingness_Pct_Range_Text AS VARCHAR(8)),'')),
			 TRIM(COALESCE(CAST(TGT.Travel_Location_Text AS VARCHAR(100)),'')),
			 Trim(Coalesce(TGT.Relocation_Willingness_Id, 0)),
			 TRIM(COALESCE(CAST(TGT.Relocation_Location_Text AS VARCHAR(500)),'')),
			 Coalesce(Trim(TGT.Calibrated_Performance_Rating_Id), 0),
			 Coalesce(Trim(TGT.Overall_Performance_Rating_Id), 0) ,
			 Coalesce(Trim(TGT.Employee_Promotability_Interest_Id), 0) ,
			 Coalesce(Trim(TGT.Potential_Performance_Id), 0),
			 Coalesce(Trim(TGT.Future_Role_1_Leadership_Level_Id), 0) ,
			 Coalesce(Trim(TGT.Future_Role_1_Org_Size_Id), 0) ,
			 Coalesce(Trim(TGT.Future_Role_1_Timeframe_Id), 0) ,
			 Coalesce(Trim(TGT.Future_Role_1_Role_Id), 0) ,
			 Coalesce(Trim(TGT.Future_Role_2_Leadership_Level_Id), 0) ,
			 Coalesce(Trim(TGT.Future_Role_2_Org_Size_Id), 0) ,
			 Trim(Coalesce(TGT.Future_Role_2_Timeframe_Id, 0)) ,
			 Coalesce(Trim(TGT.Future_Role_2_Role_Id), 0) ,
			 Coalesce(Trim(TGT.Flight_Risk_Probability_Id), 0) ,
			 Coalesce(Trim(TGT.Flight_Risk_Timeframe_Id), 0) ,
			 TRIM(COALESCE(CAST(TGT.Flight_Risk_Driver_Text AS VARCHAR(255)),'')) ,
			 TRIM(COALESCE(CAST(TGT.Flight_Risk_Secondary_Driver_Text AS VARCHAR(255)),'')) ,
			 TRIM(COALESCE(CAST(Calibration_Session_Created_Date as VARCHAR(255)),'')),
			 TRIM(COALESCE(Calibration_Session_Name, '' )),
			 TRIM(COALESCE(CAST(Calibration_Session_Last_Mod_Date_Time as VARCHAR(255)),'')),
			 TRIM(COALESCE(CAST(Calibration_Session_Published_Date as VARCHAR(255)),'')),
			 TRIM(COALESCE(Calibration_Session_Created_By_Name, '')),
			 Coalesce(Trim(TGT.Calibration_Box_Id), 0),
			 TRIM(COALESCE(Company_Tenure_Text, '')),
			 TRIM(COALESCE(Position_Tenure_Text, '')),
			 Coalesce(Trim(TGT.Lawson_Company_Num), 0) 

			
FROM EDWHR_BASE_VIEWS.EMPLOYEE_TALENT_PROFILE TGT Where TGT.Valid_To_Date = '9999-12-31')
/*And WRK.Last_Pub_Calibration_Session = WRK.Calibration_Session_Name
qualify Row_Number () Over (Partition by Employee_Num Order by Calibration_Session_Last_Mod_Date_Time desc)  = 1 */

--qualify count(*) over (partition by  Employee_Talent_Profile_SID order by Employee_Talent_Profile_SID) = 1
;



.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

UPDATE EDWHR.Employee_Talent_Profile TGT
 
SET VALID_TO_DATE =current_date - INTERVAL '1' DAY ,
 DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)
WHERE 
TGT.VALID_TO_DATE = DATE '9999-12-31'
AND  
(
TGT.Employee_Talent_Profile_SID
)
NOT IN 
(
SELECT DISTINCT Employee_Talent_Profile_SID FROM EDWHR_Staging.Employee_Talent_Profile_Wrk 
--GROUP BY 1
)
;

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;






ET;

/* End Transaction Block comment */

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','EMPLOYEE_TALENT_PROFILE');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF

