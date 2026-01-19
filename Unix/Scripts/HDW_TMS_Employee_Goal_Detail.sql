
#####################################################################################
#                                                                                   #
#   SCRIPT NAME     - HDW_TMS_Employee_Goal_Detail.sql                          #
#   Job NAME        - J_HDW_TMS_Employee_Goal_Detail                               #
#   TARGET TABLE    - EDWHR.Employee_Goal_Detail                      #
#   Developer       - Julia Kim
		                                                           #
#   Version         - 1.0 - Initial RELEASE                                         #
#   Description     - The SCRIPT loads the TARGET TABLE WITH NEW records            #
#                     AND maintain their version AS TYPE 2                          #
#                                                                                   #
#####################################################################################


bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Employee_Goal_Detail;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/* Collecting Stats on look-up table */

/*CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_SK_Xwlk');*/
CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','Employee_Perf_Goals','Trim(Individual_Goal_ID)', 'Employee_Goal_Detail');

DELETE FROM EDWHR_STAGING.Employee_Perf_Goals_REJECT;

INSERT INTO EDWHR_STAGING.Employee_Perf_Goals_REJECT
SELECT
      STG.Employee_ID,   
      STG.Goal_Title ,
      STG.Goal_Weight ,
      STG.Goal_Category ,
      STG.Expected_Result ,
      STG.Measure ,
      STG.Due_Date ,
      STG.User_Defined_Date_1,
      STG.Goal_Status ,
      STG.Goal_Progress,
      STG.Emp_Goal_Rating ,
      STG.Emp_Goal_Rating_Numeric_Value ,
      STG.Mgr_Goal_Rating,
      STG.Mgr_Goal_Rating_Numeric_Value,
      STG.Plan_Name,
      STG.Review_Period,
      STG.Review_Period_End_Date,
      STG.Review_Period_Start_Date,
      STG.Year_1,
      STG.Individual_Goal_ID,
      CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,
     
     (CASE WHEN Trim(STG.Employee_ID) IS  NULL
	    THEN 'Employee_ID is NULL'
	    WHEN Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    THEN  'Employee_Id is alpha_numeric'
    	    ELSE 0 END )
                      AS REJECT_REASON,
     'Development_Activities_Report' AS REJECT_STG_TBL_NM
     
   FROM EDWHR_STAGING.Employee_Perf_Goals STG
   where 
	     Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    
             OR  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
         AND CAST(Trim(Employee_ID) AS INT)  NOT IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee));

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;*/


DELETE FROM EDWHR_Staging.Employee_Goal_Detail_Wrk;

CALL dbadmin_procs.collect_stats_table ('EDWHR_STAGING','Employee_Goal_Detail_Wrk');

INSERT INTO EDWHR_Staging.Employee_Goal_Detail_Wrk 
(
Employee_Goal_Detail_SID
,Employee_Talent_Profile_SID
,Employee_Num
,Employee_SID
,Employee_Goal_Year_Num
,Goal_Name
,Goal_Category_Id
,Goal_Weight_Pct
,Expected_Result_Text
,Goal_Measurement_Text
,Goal_Status_Id
,Goal_Progress_Status_Id
,Goal_Performance_Plan_Id
,Goal_Due_Date
,User_Defined_Date
,Review_Year_Num
,Review_Period_End_Date
,Review_Period_Start_Date
,Review_Period_Id
,Manager_Goal_Performance_Rating_Id
,Manager_Goal_Performance_Rating_Num
,Employee_Goal_Performance_Rating_Id
,Employee_Goal_Performance_Rating_Num
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,Source_System_Code
,DW_Last_Update_Date_Time
)
SELECT
XWLK.SK AS Employee_Goal_Detail_SID ,
Trim(coalesce(ETP.Employee_Talent_Profile_SID, 0)) AS Employee_Talent_Profile_SID, 
    Cast(STG.Employee_ID as Integer) as Employee_Num ,
    emp.Employee_SID as Employee_SID,
STG.Year_1 AS Employee_Goal_Year_Num,  
Trim(STG.Goal_Title) AS Goal_Name, 
RPC.Performance_Category_Id AS Goal_Category_Id,
STG.Goal_Weight AS Goal_Weight_Pct ,
STG.Expected_Result AS Expected_Result_Text, 
STG.Measure AS Goal_Measurement_Text,
RPS.Performance_Status_Id AS Goal_Status_Id,
RPS2.Performance_Status_Id AS Goal_Progress_Status_Id,
RPN.Performance_Plan_Id  AS Goal_Performance_Plan_Id,
STG.Due_Date AS Goal_Due_Date,
STG.User_Defined_Date_1 AS User_Defined_Date,
STG.Year_1 AS Review_Year_Num,
STG.Review_Period_End_Date AS Review_Period_End_Date,
STG.Review_Period_Start_Date AS Review_Period_Start_Date,
RPP.Review_Period_Id AS Review_Period_Id,
RPR.Performance_Rating_Id AS Performance_Rating_Id,
CASE WHEN STG.Mgr_Goal_Rating_Numeric_Value = 'Not Applicable' 
			   OR STG.Mgr_Goal_Rating_Numeric_Value= 'Not Rated' THEN null  ELSE STG.Mgr_Goal_Rating_Numeric_Value END  AS Manager_Goal_Performance_Rating_Num,
RPR2.Performance_Rating_Id AS Employee_Goal_Performance_Rating_Id,
CASE WHEN STG.Emp_Goal_Rating_Numeric_Value = 'Not Applicable' 
			   OR STG.Emp_Goal_Rating_Numeric_Value = 'Not Rated' THEN null  ELSE STG.Emp_Goal_Rating_Numeric_Value END  AS Employee_Goal_Performance_Rating_Num,
Trim(Coalesce(ETP.Lawson_Company_Num, 0)) AS Lawson_Company_Num,
Trim(coalesce(ETP.Process_Level_Code, '00000')) AS Process_Level_Code,
STG.Individual_Goal_ID AS Source_System_Key,
'M' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM
EDWHR_STAGING.Employee_Perf_Goals STG

INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
ON trim(STG.Individual_Goal_ID) = XWLK.SK_Source_Txt AND XWLK.SK_Type = 'Employee_Goal_Detail'


INNER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(STG.Employee_ID AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31'
	AND EMP.Lawson_Company_Num = Cast(Substr(STG.Job_Code,1,4) AS INTEGER)

LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee_Talent_Profile ETP
ON 
(CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG.Employee_ID) AS INT) ELSE 0 END )= Trim(ETP.Employee_num)
	AND ETP.Valid_To_Date='9999-12-31'


LEFT JOIN  EDWHR_BASE_VIEWS.Ref_Performance_Category RPC
ON RPC.Performance_Category_Desc = STG.Goal_Category

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Status RPS
ON RPS.Performance_Status_Desc = STG.Goal_Status

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Status RPS2
ON RPS2.Performance_Status_Desc = STG.Goal_Progress

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Plan RPN
ON RPN.Performance_Plan_Desc = STG.Plan_Name

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Period RPP
ON RPP.Review_Period_Desc = STG.Review_Period

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating RPR
ON RPR.Performance_Rating_Desc = STG.Mgr_Goal_Rating

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating RPR2
ON RPR2.Performance_Rating_Desc = STG.Emp_Goal_Rating

WHERE emp.Employee_SID <> 0
qualify row_number() over (partition by Employee_Goal_Detail_SID order by Employee_Goal_Detail_SID)= 1

;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/* Collecting Stats on work table/s */

CALL dbadmin_procs.collect_stats_table ('EDWHR_STAGING','Employee_Goal_Detail_Wrk');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

BT;







UPDATE TGT
FROM EDWHR.Employee_Goal_Detail TGT,
(select * from  EDWHR_Staging.Employee_Goal_Detail_Wrk ) WRK
SET  Valid_To_Date = Current_Date  - INTERVAL '1' DAY,
			 DW_Last_Update_Date_Time = Current_Timestamp(0)
WHERE 
			Trim(TGT.Employee_Goal_Detail_SID) = Trim(WRK.Employee_Goal_Detail_SID)
AND (
			Trim(Coalesce(TGT.Employee_Talent_Profile_SID,0)) <>Trim(Coalesce(WRK.Employee_Talent_Profile_SID,0))
			OR Trim(Coalesce(TGT.Employee_Goal_Year_Num,0)) <>Trim(Coalesce(WRK.Employee_Goal_Year_Num,0)) 
			OR Trim(Coalesce(TGT.Employee_SID,0)) <>Trim(Coalesce(WRK.Employee_SID,0))
			OR Trim(Coalesce(TGT.Employee_Num,0)) <>Trim(Coalesce(WRK.Employee_Num,0)) 
			OR TRIM(COALESCE(CAST(Trim(TGT.Goal_Name) AS VARCHAR(250)),'')) <> TRIM(COALESCE(CAST(Trim(WRK.Goal_Name) AS VARCHAR(250)),''))
			OR TRIM(Coalesce(TGT.Goal_Category_Id,0)) <>TRIM(Coalesce(WRK.Goal_Category_Id, 0))
			OR TRIM(Coalesce(TGT.Goal_Weight_Pct,0)) <>TRIM(Coalesce(WRK.Goal_Weight_Pct, 0))
			OR TRIM(COALESCE(CAST(TGT.Expected_Result_Text AS VARCHAR(10000)),'')) <> TRIM(COALESCE(CAST(WRK.Expected_Result_Text AS VARCHAR(10000)),''))
			OR TRIM(COALESCE(CAST(TGT.Goal_Measurement_Text AS VARCHAR(10000)),'')) <> TRIM(COALESCE(CAST(WRK.Goal_Measurement_Text AS VARCHAR(10000)),''))
		    OR TRIM(Coalesce(TGT.Goal_Status_Id,0)) <>TRIM(Coalesce(WRK.Goal_Status_Id, 0))
			OR Trim(Coalesce(TGT.Goal_Progress_Status_Id, 0)) <>Trim(Coalesce(WRK.Goal_Progress_Status_Id, 0)) 
			OR Trim(Coalesce(TGT.Goal_Performance_Plan_Id,0)) <>Trim(Coalesce(WRK.Goal_Performance_Plan_Id,0))
			OR TRIM(TGT.Goal_Due_Date) <> TRIM(WRK.Goal_Due_Date)
			OR TRIM(TGT.User_Defined_Date)  <> TRIM(WRK.User_Defined_Date )
			OR Trim(Coalesce(TGT.Review_Year_Num,0)) <>Trim(Coalesce(WRK.Review_Year_Num, 0))
			OR TRIM(TGT.Review_Period_End_Date)  <> TRIM(WRK.Review_Period_End_Date )
			OR TRIM(TGT.Review_Period_Start_Date) <> TRIM(WRK.Review_Period_Start_Date)
			OR Trim(Coalesce(TGT.Review_Period_Id,0))  <>Trim(Coalesce(WRK.Review_Period_Id, 0))  
			OR Trim(Coalesce(TGT.Manager_Goal_Performance_Rating_Id,0)) <>Trim(Coalesce(WRK.Manager_Goal_Performance_Rating_Id, 0))
			OR Trim(Coalesce(TGT.Manager_Goal_Performance_Rating_Num,0))<>Trim(Coalesce(WRK.Manager_Goal_Performance_Rating_Num,0))
			OR Trim(Coalesce(TGT.Employee_Goal_Performance_Rating_Id,0)) <>Trim(Coalesce(WRK.Employee_Goal_Performance_Rating_Id, 0)) 
			OR Trim(Coalesce(TGT.Employee_Goal_Performance_Rating_Num,0)) <> Trim(Coalesce(WRK.Employee_Goal_Performance_Rating_Num,0) )
			OR Trim(Coalesce(TGT.Lawson_Company_Num,0))<>Trim(Coalesce(WRK.Lawson_Company_Num,0))
			OR Trim(Coalesce(TGT.Process_Level_Code,0))<>Trim(Coalesce(WRK.Process_Level_Code,0))
			OR TRIM(COALESCE(CAST(TGT.Source_System_Key AS VARCHAR(100)),'')) <> TRIM(COALESCE(CAST(WRK.Source_System_Key AS VARCHAR(100)),''))
			 
			)
AND TGT.Valid_TO_Date = DATE '9999-12-31'

;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;



INSERT INTO EDWHR.Employee_Goal_Detail 
(
Employee_Goal_Detail_SID
,Valid_From_Date
, Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,Employee_Goal_Year_Num
,Goal_Name
,Goal_Category_Id
,Goal_Weight_Pct
,Expected_Result_Text
,Goal_Measurement_Text
,Goal_Status_Id
,Goal_Progress_Status_Id
,Goal_Performance_Plan_Id
,Goal_Due_Date
,User_Defined_Date
,Review_Year_Num
,Review_Period_End_Date
,Review_Period_Start_Date
,Review_Period_Id
,Manager_Goal_Performance_Rating_Id
,Manager_Goal_Performance_Rating_Num
,Employee_Goal_Performance_Rating_Id
,Employee_Goal_Performance_Rating_Num
,Lawson_Company_Num
,Process_Level_Code
,Valid_To_Date
,Source_System_Key
,Source_System_Code
,DW_Last_Update_Date_Time
)
SELECT
WRK.Employee_Goal_Detail_SID ,
     Current_Date AS Valid_From_Date, 
 	WRK.Employee_SID,
	 WRK.Employee_Num,
      WRK.Employee_Talent_Profile_SID ,
      WRK.Employee_Goal_Year_Num ,
      Trim(WRK.Goal_Name),
      WRK.Goal_Category_Id ,
      WRK.Goal_Weight_Pct ,
      WRK.Expected_Result_Text ,
      WRK.Goal_Measurement_Text ,
      WRK.Goal_Status_Id ,
      WRK.Goal_Progress_Status_Id ,
      WRK.Goal_Performance_Plan_Id ,
      WRK.Goal_Due_Date ,
      WRK.User_Defined_Date ,
      WRK.Review_Year_Num ,
      WRK.Review_Period_End_Date,
      WRK.Review_Period_Start_Date ,
      WRK.Review_Period_Id ,
      WRK.Manager_Goal_Performance_Rating_Id,
      WRK.Manager_Goal_Performance_Rating_Num ,
      WRK.Employee_Goal_Performance_Rating_Id ,
      WRK.Employee_Goal_Performance_Rating_Num ,
      Trim(Coalesce(WRK.Lawson_Company_Num,0)),
      WRK.Process_Level_Code ,
      '9999-12-31' AS Valid_To_Date,
      WRK.Source_System_Key ,
      WRK.Source_System_Code,
      Current_Timestamp(0) As DW_Last_Update_Date_Time
FROM EDWHR_STAGING.Employee_Goal_Detail_Wrk WRK
	
WHERE
     (WRK.Employee_Goal_Detail_SID ,
      WRK.Employee_Talent_Profile_SID ,
 	WRK.Employee_SID,
 	WRK.Employee_Num,
      WRK.Employee_Goal_Year_Num ,
      Trim(WRK.Goal_Name) ,
      WRK.Goal_Category_Id ,
      WRK.Goal_Weight_Pct ,
      TRIM(WRK.Expected_Result_Text) ,
      TRIM(WRK.Goal_Measurement_Text) ,
      WRK.Goal_Status_Id ,
      WRK.Goal_Progress_Status_Id ,
      WRK.Goal_Performance_Plan_Id ,
      WRK.Goal_Due_Date ,
      WRK.User_Defined_Date ,
      WRK.Review_Year_Num ,
      WRK.Review_Period_End_Date,
      WRK.Review_Period_Start_Date ,
      WRK.Review_Period_Id ,
      WRK.Manager_Goal_Performance_Rating_Id,
      WRK.Manager_Goal_Performance_Rating_Num ,
      WRK.Employee_Goal_Performance_Rating_Id ,
      WRK.Employee_Goal_Performance_Rating_Num ,
      Trim(Coalesce(WRK.Lawson_Company_Num,0)),
      WRK.Process_Level_Code,
      WRK.Source_System_Key ,
      WRK.Source_System_Code
      )
NOT IN 
      (
     SELECT 
      TGT.Employee_Goal_Detail_SID ,
      TGT.Employee_Talent_Profile_SID ,
 	TGT.Employee_SID,
 	TGT.Employee_Num,
      TGT.Employee_Goal_Year_Num ,
      Trim(TGT.Goal_Name) ,
      TGT.Goal_Category_Id ,
      TGT.Goal_Weight_Pct ,
      TRIM(TGT.Expected_Result_Text) ,
      TRIM(TGT.Goal_Measurement_Text) ,
      TGT.Goal_Status_Id ,
      TGT.Goal_Progress_Status_Id ,
      TGT.Goal_Performance_Plan_Id ,
      TGT.Goal_Due_Date ,
      TGT.User_Defined_Date ,
      TGT.Review_Year_Num ,
      TGT.Review_Period_End_Date,
      TGT.Review_Period_Start_Date ,
      TGT.Review_Period_Id ,
      TGT.Manager_Goal_Performance_Rating_Id,
      TGT.Manager_Goal_Performance_Rating_Num ,
      TGT.Employee_Goal_Performance_Rating_Id ,
      TGT.Employee_Goal_Performance_Rating_Num ,
      Trim(Coalesce(TGT.Lawson_Company_Num,0)),
      TGT.Process_Level_Code ,
      TGT.Source_System_Key ,
      TGT.Source_System_Code
   FROM EDWHR_BASE_VIEWS.Employee_Goal_Detail TGT WHERE TGT.Valid_To_Date = '9999-12-31')
;

   
 .IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

UPDATE EDWHR.Employee_Goal_Detail TGT 
SET VALID_TO_DATE =current_date - INTERVAL '1' DAY ,
 DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)
WHERE 
TGT.VALID_TO_DATE = DATE '9999-12-31'
AND  
(
TGT.Employee_Goal_Detail_SID)
NOT IN 
(
SELECT DISTINCT WRK.Employee_Goal_Detail_SID FROM EDWHR_Staging.Employee_Goal_Detail_Wrk WRK)
;

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;


ET;

/* End Transaction Block comment */

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Employee_Goal_Detail');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;










   