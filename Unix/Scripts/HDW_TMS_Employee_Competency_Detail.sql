
#####################################################################################
#                                               					#
#   Script Name     - HDW_TMS_Employee_Competency_Detail                 		#
#   Job Name    	- J_HDW_TMS_Employee_Competency_Detail                         	#
#   Target Table    - EDWHR.Employee_Competency_Detail                     		#
#   Developer   	- Julia Kim                                 			#
#   Version 		- 1.0 - Initial Release                     			#
#   Description 	- The script loads the Target table from the staging table	#
#####################################################################################


bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Employee_Competency_Detail;' FOR SESSION;



.IF ERRORCODE <> 0 Then .Quit ERRORCODE;





CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','COMPETENCY_RATINGS_REPORT','Coalesce(Trim(COMP_Record_ID), '''')', 'Employee_Competency_Detail');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;



/* Collecting Stats on source table */

CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','COMPETENCY_RATINGS_REPORT');
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/* Collecting Stats on source table */

CALL dbadmin_procs.collect_stats_table ('EDWHR','Employee_Talent_Profile');
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/* Collecting Stats on source table */

CALL dbadmin_procs.collect_stats_table ('EDWHR','Ref_Performance_Plan');
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;


/* Collecting Stats on source table */

CALL dbadmin_procs.collect_stats_table ('EDWHR','Ref_Competency_Group');
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/* Collecting Stats on source table */

CALL dbadmin_procs.collect_stats_table ('EDWHR','Ref_Competency');
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/* Collecting Stats on source table */

CALL dbadmin_procs.collect_stats_table ('EDWHR','Ref_Performance_Status');
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/* Collecting Stats on source table */

CALL dbadmin_procs.collect_stats_table ('EDWHR','Ref_Performance_Period');
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;


/* Collecting Stats on source table */

CALL dbadmin_procs.collect_stats_table ('EDWHR','Ref_Performance_Rating');
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/* INSERTING INTO THe REJECT TABLE 'Employee_Id not in Employee_Talent_Profile'*/

DELETE FROM EDWHR_STAGING.COMPETENCY_RATINGS_REPORT_REJECT;
INSERT INTO EDWHR_STAGING.COMPETENCY_RATINGS_REPORT_REJECT
SELECT
      STG.Employee_ID,   
      STG.Review_Period ,
      STG.Review_Period_Start_Date ,
      STG.Review_Period_End_Date ,
      STG.Review_Year ,
      STG.Plan_Name ,
      STG.Competency_Group ,
      STG.Competency,
      STG. Employee_Rating_Numeric_Value ,
      STG.Employee_Rating_Scale_Value ,
      STG.Manager_Rating_Numeric_Value ,
      STG.Manager_Rating_Scale_Value ,
      STG.Manager_Employee_Gap ,
      STG.Evaluation_Workflow_State,
      STG.COMP_Record_ID,
      'Employee_Id not in Employee'AS REJECT_REASON,
     'COMPETENCY_RATINGS_REPORT' AS REJECT_STG_TBL_NM,
     CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
   FROM Edwhr_Staging.COMPETENCY_RATINGS_REPORT STG
   where  Trim(STG.Employee_ID) NOT IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee)
;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE; 




DELETE From  EDWHR_STAGING.Employee_Competency_Detail_Wrk ;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;






INSERT INTO EDWHR_Staging.Employee_Competency_Detail_Wrk 
(
Employee_Competency_Result_SID
,Employee_Talent_Profile_SID
 ,Employee_SID
 ,Employee_Num
,Performance_Plan_Id
,Competency_Group_Id
,Competency_Id
,Evaluation_Workflow_Status_Id
,Review_Period_Id
,Review_Year_Num
,Review_Period_Start_Date
,Review_Period_End_Date
,Employee_Rating_Num
,Employee_Rating_Id
,Manager_Rating_Num
,Manager_Rating_Id
,Manager_Employee_Rating_Gap_Num
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,Source_System_Code
,DW_Last_Update_Date_Time   
)
SELECT
XWLK.SK AS Employee_Competency_Result_SID ,
ETP.Employee_Talent_Profile_SID AS Employee_Talent_Profile_SID ,
 emp.Employee_SID as Employee_SID,
 Cast(STG.Employee_ID as Integer) as Employee_Num ,  
RPP1.Performance_Plan_Id AS Performance_Plan_Id , 
RCG.Competency_Group_Id AS Competency_Group_Id ,
RC.Competency_Id AS Competency_Id ,
RPS.Performance_Status_Id AS Evaluation_Workflow_Status_Id ,
RPP2.Review_Period_Id AS Review_Period_Id ,
STG.Review_Year AS Review_Year_Num,
STG.Review_Period_Start_Date AS Review_Period_Start_Date,
STG.Review_Period_End_Date AS Review_Period_End_Date,
CASE WHEN STG.Employee_Rating_Numeric_Value = 'Not Rated' OR STG.Employee_Rating_Numeric_Value ='Not Applicable' 
THEN NULL
ELSE STG.Employee_Rating_Numeric_Value END
AS Employee_Rating_Num,
RPR.Performance_Rating_Id AS Employee_Rating_Id,
CASE WHEN STG.Manager_Rating_Numeric_Value = 'Not Rated' OR STG.Manager_Rating_Numeric_Value = 'Not Applicable' 
THEN NULL 
ELSE STG.Manager_Rating_Numeric_Value END  AS Manager_Rating_Num,
RPR2.Performance_Rating_Id AS Manager_Rating_Id,
CASE WHEN STG. Manager_Employee_Gap=  'Not Computed' 
THEN NULL
ELSE STG. Manager_Employee_Gap END
AS Manager_Employee_Rating_Gap_Num,
ETP.Lawson_Company_Num AS Lawson_Company_Num,
--ETP.Process_Level_Code AS Process_Level_Code,
Coalesce(ETP.Process_Level_Code,'00000') AS Process_Level_Code,
STG.COMP_Record_ID AS Source_System_Key,
'M' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM
EDWHR_STAGING.COMPETENCY_RATINGS_REPORT STG

INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
ON trim(STG.COMP_Record_ID) = XWLK.SK_Source_Txt AND XWLK.SK_Type = 'Employee_Competency_Detail'

	INNER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(STG.Employee_ID AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31'
	AND EMP.Lawson_Company_Num = Cast(Substr(STG.Job_Code,1,4) AS INTEGER)

LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee_Talent_Profile ETP
ON(
TRIM(ETP.Employee_num) = Trim(STG.Employee_ID)
)
AND ETP.Valid_To_Date='9999-12-31'

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Plan RPP1
ON (
TRIM(RPP1.PERFORMANCE_PLAN_DESC) =  TRIM(STG.Plan_Name)
)
LEFT JOIN EDWHR_BASE_VIEWS.Ref_Competency_Group RCG
ON (
TRIM(RCG.Competency_Group_Desc) = TRIM(STG.Competency_Group)
)
LEFT JOIN EDWHR_BASE_VIEWS.Ref_Competency RC
ON (
TRIM(RC.Competency_Desc) = TRIM(STG.Competency)
)
LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Status RPS
ON (
TRIM(RPS.Performance_Status_Desc) = TRIM(STG.Evaluation_Workflow_State)
)
LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Period RPP2
ON(
TRIM(RPP2.Review_Period_Desc) = TRIM(STG.Review_Period)
)
LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating RPR
ON (
TRIM(RPR.Performance_Rating_Desc) = TRIM(STG.Employee_Rating_Scale_Value)
)
LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating RPR2
ON (
TRIM(RPR2.Performance_Rating_Desc)= TRIM(STG.Manager_Rating_Scale_Value)
)
qualify row_number() over (partition by Employee_Competency_Result_SID order by Employee_Competency_Result_SID) =1
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;




/* Collecting Stats on work table/s */

CALL dbadmin_procs.collect_stats_table ('EDWHR_STAGING','Employee_Competency_Detail_Wrk');

BT;


UPDATE TGT
FROM EDWHR.Employee_Competency_Detail TGT,
(select * from  EDWHR_Staging.Employee_Competency_Detail_Wrk) WRK
SET  Valid_To_Date = Current_Date  - INTERVAL '1' DAY,
			 DW_Last_Update_Date_Time = Current_Timestamp(0)
WHERE 
			Trim(TGT.Employee_Competency_Result_SID) = Trim(WRK.Employee_Competency_Result_SID)
AND (
			Trim(Coalesce(TGT.Employee_Talent_Profile_SID,0)) <>Trim(Coalesce(WRK.Employee_Talent_Profile_SID,0))
			OR Trim(Coalesce(TGT.Performance_Plan_Id,0)) <>Trim(Coalesce(WRK.Performance_Plan_Id,0)) 
			OR Trim(Coalesce(TGT.Employee_SID,0)) <>Trim(Coalesce(WRK.Employee_SID,0)) 
			OR Trim(Coalesce(TGT.Employee_Num,0)) <>Trim(Coalesce(WRK.Employee_Num,0)) 
			OR TRIM(COALESCE(CAST(TGT.Competency_Group_Id AS VARCHAR(7)),'')) <> TRIM(COALESCE(CAST(WRK.Competency_Group_Id AS VARCHAR(7)),''))
			OR TRIM(Coalesce(TGT.Competency_Id,0)) <>TRIM(Coalesce(WRK.Competency_Id, 0))
			OR Trim(Coalesce(TGT.Evaluation_Workflow_Status_Id, 0)) <>Trim(Coalesce(WRK.Evaluation_Workflow_Status_Id, 0)) 
			OR Trim(Coalesce(TGT.Review_Period_Id,0)) <>Trim(Coalesce(WRK.Review_Period_Id,0))
			OR Trim(Coalesce(TGT.Review_Year_Num,0)) <>Trim(Coalesce(WRK.Review_Year_Num, 0))
			OR TRIM(TGT.Review_Period_Start_Date) <> TRIM(WRK.Review_Period_Start_Date)
			OR TRIM(TGT.Review_Period_End_Date)  <> TRIM(WRK.Review_Period_End_Date )
			OR Trim(Coalesce(TGT.Employee_Rating_Num,0))  <>Trim(Coalesce(WRK.Employee_Rating_Num, 0))  
			OR Trim(Coalesce(TGT.Employee_Rating_Id,0)) <>Trim(Coalesce(WRK.Employee_Rating_Id, 0))
			OR Trim(Coalesce(TGT.Manager_Rating_Num,0))<>Trim(Coalesce(WRK.Manager_Rating_Num,0))
			OR Trim(Coalesce(TGT.Manager_Rating_Id,0)) <>Trim(Coalesce(WRK.Manager_Rating_Id, 0)) 
			OR Trim(Coalesce(TGT.Manager_Employee_Rating_Gap_Num,0)) <> Trim(Coalesce(WRK.Manager_Employee_Rating_Gap_Num,0) )
			OR Trim(Coalesce(TGT.Lawson_Company_Num,0))<>Trim(Coalesce(WRK.Lawson_Company_Num,0))
			OR Trim(Coalesce(TGT.Process_Level_Code,0))<>Trim(Coalesce(WRK.Process_Level_Code,0))
			OR TRIM(COALESCE(CAST(TGT.Source_System_Key AS VARCHAR(100)),'')) <> TRIM(COALESCE(CAST(WRK.Source_System_Key AS VARCHAR(100)),''))

			 
			)
AND TGT.Valid_TO_Date = DATE '9999-12-31'

;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;



INSERT INTO EDWHR.Employee_Competency_Detail 
(
Employee_Competency_Result_SID,
Valid_From_Date,
 Employee_SID,
 Employee_Num,
Employee_Talent_Profile_SID,
Performance_Plan_Id,
Competency_Group_Id,
Competency_Id,
Evaluation_Workflow_Status_Id,
Review_Period_Id,
Review_Year_Num,
Review_Period_Start_Date,
Review_Period_End_Date,
Employee_Rating_Num,
Employee_Rating_Id,
Manager_Rating_Num,
Manager_Rating_Id,
Manager_Employee_Rating_Gap_Num,
Lawson_Company_Num,
Process_Level_Code,
Valid_To_Date,
Source_System_Key,
Source_System_Code,
DW_Last_Update_Date_Time
)
SELECT
WRK.Employee_Competency_Result_SID,
Current_Date AS Valid_From_Date,
 WRK.Employee_SID,
 WRK.Employee_Num,
WRK.Employee_Talent_Profile_SID,
WRK.Performance_Plan_Id,
WRK.Competency_Group_Id,
WRK.Competency_Id,
WRK.Evaluation_Workflow_Status_Id,
WRK.Review_Period_Id,
WRK.Review_Year_Num,
WRK.Review_Period_Start_Date,
WRK.Review_Period_End_Date,
WRK.Employee_Rating_Num,
WRK.Employee_Rating_Id,
WRK.Manager_Rating_Num,
WRK.Manager_Rating_Id,
WRK.Manager_Employee_Rating_Gap_Num,
WRK.Lawson_Company_Num,
WRK.Process_Level_Code,
--Coalesce(WRK.Process_Level_Code,'00000') as Process_Level_Code,
'9999-12-31' AS Valid_To_Date,
Trim(WRK.Source_System_Key),
WRK.Source_System_Code,
Current_Timestamp(0) As DW_Last_Update_Date_Time
FROM EDWHR_Staging.Employee_Competency_Detail_Wrk WRK
WHERE
(

WRK.Employee_Competency_Result_SID,
WRK.Employee_Talent_Profile_SID,
 WRK.Employee_SID,
 WRK.Employee_Num,
WRK.Performance_Plan_Id,
WRK.Competency_Group_Id,
WRK.Competency_Id,
WRK.Evaluation_Workflow_Status_Id,
WRK.Review_Period_Id,
WRK.Review_Year_Num,
WRK.Review_Period_Start_Date,
WRK.Review_Period_End_Date,
WRK.Employee_Rating_Num,
WRK.Employee_Rating_Id,
WRK.Manager_Rating_Num,
WRK.Manager_Rating_Id,
WRK.Manager_Employee_Rating_Gap_Num,
WRK.Lawson_Company_Num,
WRK.Process_Level_Code,
Trim(WRK.Source_System_Key),
WRK.Source_System_Code
)
NOT IN
(
SELECT 

TGT.Employee_Competency_Result_SID,
TGT.Employee_Talent_Profile_SID,
TGT.Employee_SID,
TGT.Employee_Num,
TGT.Performance_Plan_Id,
TGT.Competency_Group_Id,
TGT.Competency_Id,
TGT.Evaluation_Workflow_Status_Id,
TGT.Review_Period_Id,
TGT.Review_Year_Num,
TGT.Review_Period_Start_Date,
TGT.Review_Period_End_Date,
TGT.Employee_Rating_Num,
TGT.Employee_Rating_Id,
TGT.Manager_Rating_Num,
TGT.Manager_Rating_Id,
TGT.Manager_Employee_Rating_Gap_Num,
TGT.Lawson_Company_Num,
TGT.Process_Level_Code,
Trim(TGT.Source_System_Key),
TGT.Source_System_Code
FROM EDWHR_BASE_VIEWS.Employee_Competency_Detail TGT Where TGT.Valid_To_Date = '9999-12-31')

;




.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

UPDATE EDWHR.Employee_Competency_Detail TGT
 
SET VALID_TO_DATE =current_date - INTERVAL '1' DAY ,
 DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)
WHERE 
TGT.VALID_TO_DATE = DATE '9999-12-31'
AND  
(
TGT.Employee_Competency_Result_SID)
NOT IN 
(
SELECT DISTINCT Employee_Competency_Result_SID FROM EDWHR_Staging.Employee_Competency_Detail_Wrk
--GROUP BY 1
)
;

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;






ET;

/* End Transaction Block comment */

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Employee_Competency_Detail');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF

			
