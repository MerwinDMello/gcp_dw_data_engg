################################################################################
#    Target Table:    <<EDWHR.Employee_Performance_Detail>>                    #
# This script loads data from EDWHR_STAGING.PERFORMANCE_RATINGS_REPORT         #
# and inserts into the reference table using BTEQ                              #
#                                                                              #
################################################################################################
# Change Control:                                                                              #
#                                                                                              #
# Date                                     INITIAL RELEASE                                     #
# 04/02/2018 Skylar Youngblood              Initial version                                    #
# 05/09/2018 Julia Kim 	           created correct reject table                                #
# 08/14/2018 Skylar Youngblood     Update Retiring logic to match on Source_System_Keys        #
################################################################################################

bteq << EOF >> $1;

.SET ERROROUT STDOUT;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Employee_Performance_Detail;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/*Insert Bad character records and the records that are left out after inner join with EDWHR.Employee_Talent_Profile into Reject*/
DELETE EDWHR_STAGING.PERFORMANCE_RATINGS_REPORT_Reject;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
INSERT INTO EDWHR_STAGING.PERFORMANCE_RATINGS_REPORT_Reject 
SELECT



      STG.Employee_ID ,
      STG.Rev_Period ,
      STG.Rev_Period_Start_Date ,
      STG.Rev_Period_End_Date ,
      STG.Rev_Year ,
      STG.Plan_Name ,
      STG.Eval_Workflow_State ,
      STG.Manager_Record ,
      STG.Manager_Employee_ID_Record ,
      STG.Emp_Perf_Rat_Num_Val ,
      STG.Emp_Perf_Rat_Scale_Val ,
      STG.Perf_Rat_Numeric_Val ,
      STG.Perf_Rat_Scale_Val ,
      STG.Emp_Smry_Comp_Rat_Num_Val ,
      STG.Emp_Smry_Comp_Rat_Scale_Val ,
      STG.Sumry_Comp_Rat_Numeric_Val ,
      STG.Sumry_Comp_Rat_Scale_Val ,
      STG.Emp_Smry_Goal_Rat_Num_Val ,
      STG.Emp_Smry_Goal_Rat_Scale_Val ,
      STG.Smry_Goal_Rating_Num_Val ,
      STG.Smry_Goal_Rat_Scale_Val ,
      STG.Emp_Strengths_Accomplishments ,
      STG.Emp_Area_Improvement ,
      STG.Employee_Additional_Comments ,
      STG.Strengths_Accomplishments ,
      STG.Areas_Improvement ,
      STG.Additional_Comments ,
      STG.Evaluation_Record_ID ,
      CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,
      (CASE WHEN Trim(STG.Employee_ID) IS  NULL
	    THEN 'Employee_ID is NULL'
	    WHEN Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    THEN  'Employee_Id is alpha_numeric'
    	    ELSE 0 END )
                      AS REJECT_REASON,
     'Employee_Performance_Detail' AS REJECT_STG_TBL_NM

From EDWHR_STAGING.PERFORMANCE_RATINGS_REPORT STG
WHERE
     Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
     OR length(Trim(STG.Employee_ID)) >= 13 
     OR  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
     AND CAST(Trim(Employee_ID) AS INT)  NOT IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee));
	 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


 CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR_STAGING','PERFORMANCE_RATINGS_REPORT');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DELETE FROM EDWHR_Staging.Employee_Performance_Detail_WRK;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT INTO EDWHR_Staging.Employee_Performance_Detail_WRK
(
Evaluation_Record_ID,
Employee_Num,
Employee_SID,
Valid_From_Date,
Employee_Talent_Profile_SID,
Review_Period_Id,
Review_Year_Num,
Review_Period_Start_Date,
Review_Period_End_Date,
Performance_Plan_Id,
Evaluation_Workflow_Status_Id,
Manager_Full_Name,
Manager_Employee_Num,
Employee_Performance_Num,
Employee_Performance_Rating_Id,
Performance_Rating_Num,
Performance_Rating_Id,
Employee_Smry_Competency_Num,
Employee_Smry_Competency_Rating_Id,
Smry_Competency_Num,
Smry_Competency_Rating_Id,
Employee_Smry_Goal_Num,
Employee_Smry_Goal_Rating_Id,
Smry_Goal_Num,
Smry_Goal_Rating_Id,
Employee_Strength_Accomplishment_Text,
Employee_Area_of_Improvement_Text,
Employee_Additional_Comment_Text,
Strength_Accomplishment_Text,
Areas_of_Improvement_Text,
Additional_Comment_Text,
Lawson_Company_Num,
Process_Level_Code,
Source_System_Key,
Valid_To_Date,
Source_System_Code,
DW_Last_Update_Date_Time
)
SELECT 
    STG.Evaluation_Record_ID,
    Cast(STG.Employee_ID as Integer) as Employee_Num ,
    emp.Employee_SID as Employee_SID,
    DATE AS Valid_From_Date,
    Tal.Employee_Talent_Profile_SID,
    Per.Review_Period_Id,
    CAST(TO_NUMBER(STG.Rev_Year) AS INTEGER),
    STG.Rev_Period_Start_Date,
    STG.Rev_Period_End_Date,
    Plan.Performance_Plan_Id,
    Status.Performance_Status_Id,
    STG.Manager_Record,
    CAST(TO_NUMBER(STG.Manager_Employee_ID_Record) AS INTEGER) AS Manager_Employee_Num,
    CAST(TO_NUMBER(STG.Emp_Perf_Rat_Num_Val) AS INTEGER) AS Employee_Performance_Num,
    Rat.Performance_Rating_Id,
    CASE WHEN STG.Perf_Rat_Numeric_Val IN ( 'Not Rated', 'Not Aplicable') THEN NULL ELSE CAST(TO_NUMBER(STG.Perf_Rat_Numeric_Val ) AS INTEGER) END AS Performance_Rating_Num,
    Perf.Performance_Rating_Id,
    CASE WHEN STG.Emp_Smry_Comp_Rat_Num_Val = 'Not Computed' THEN NULL ELSE CAST(STG.Emp_Smry_Comp_Rat_Num_Val AS DECIMAL(5,2)) END AS Employee_Smry_Competency_Num,
    Comp.Performance_Rating_Id,
    CASE WHEN STG.Sumry_Comp_Rat_Numeric_Val = 'Not Computed' THEN NULL ELSE CAST(STG.Sumry_Comp_Rat_Numeric_Val AS DECIMAL(5,2)) END AS Employee_Smry_Goal_Num,
    Scale.Performance_Rating_Id,
    CASE WHEN STG.Emp_Smry_Goal_Rat_Num_Val =   'Not Computed' THEN NULL ELSE CAST(STG.Emp_Smry_Goal_Rat_Num_Val AS DECIMAL(5,2)) END AS Smry_Competency_Num,
    Goal.Performance_Rating_Id,
    CASE WHEN STG.Smry_Goal_Rating_Num_Val =  'Not Computed' THEN NULL ELSE CAST(STG.Smry_Goal_Rating_Num_Val AS DECIMAL(5,2)) END AS Smry_Goal_Num,
    Smry.Performance_Rating_Id,
    STG.Emp_Strengths_Accomplishments,
    STG.Emp_Area_Improvement,
    STG.Employee_Additional_Comments,
    STG.Strengths_Accomplishments,
    STG.Areas_Improvement,
    STG.Additional_Comments,
    Coalesce(Tal.Lawson_Company_Num, 0),
    Coalesce(Tal.Process_Level_Code, '00000'),
    STG.Evaluation_Record_ID,
    '9999-12-31' AS Valid_To_Date,
    'M' AS Source_System_Code,
    CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
    
    FROM EDWHR_STAGING.PERFORMANCE_RATINGS_REPORT STG
	
	INNER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(STG.Employee_ID AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31' 
	AND EMP.Lawson_Company_Num = Cast(Substr(STG.Job_Code,1,4) AS INTEGER)

    LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee_Talent_Profile Tal
    ON CAST(STG.Employee_ID AS BIGINT) = Tal.Employee_Num
	AND Tal.Valid_To_Date='9999-12-31' 
    
    LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Period Per
    ON STG.Rev_Period = Per.Review_Period_Desc
    
    LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Plan Plan
    ON STG.Plan_Name = Plan.Performance_Plan_Desc
    
   LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Status Status
    ON STG.Eval_Workflow_State = Status.Performance_Status_Desc

    LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating Rat
    ON STG.Emp_Perf_Rat_Scale_Val = Rat.Performance_Rating_Desc

    LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating Perf
    ON STG.Perf_Rat_Scale_Val = Perf.Performance_Rating_Desc
    
   LEFT JOIN  EDWHR_BASE_VIEWS.Ref_Performance_Rating Comp
    ON STG.Emp_Smry_Comp_Rat_Scale_Val = Comp.Performance_Rating_Desc
    
    LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating Scale
    ON STG.Sumry_Comp_Rat_Scale_Val = Scale.Performance_Rating_Desc
    
    LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating Goal
    ON STG.Emp_Smry_Goal_Rat_Scale_Val = Goal.Performance_Rating_Desc
    
    LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Rating Smry
    ON STG.Smry_Goal_Rat_Scale_Val = Smry.Performance_Rating_Desc

    WHERE isnumeric(COALESCE(STG.Employee_ID,0))=0
     AND STG.Employee_ID is not null;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

 CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR_STAGING','Employee_Performance_Detail_WRK');
 
 .IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DELETE FROM EDWHR_Staging.Employee_Performance_Detail_WRK1;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


INSERT INTO EDWHR_Staging.Employee_Performance_Detail_WRK1
(
Employee_Performance_SID,
Employee_SID,
 Employee_Num,
Valid_From_Date,
Employee_Talent_Profile_SID,
Review_Period_Id,
Review_Year_Num,
Review_Period_Start_Date,
Review_Period_End_Date,
Performance_Plan_Id,
Evaluation_Workflow_Status_Id,
Manager_Full_Name,
Manager_Employee_Num,
Employee_Performance_Num,
Employee_Performance_Rating_Id,
Performance_Rating_Num,
Performance_Rating_Id,
Employee_Smry_Competency_Num,
Employee_Smry_Competency_Rating_Id,
Smry_Competency_Num,
Smry_Competency_Rating_Id,
Employee_Smry_Goal_Num,
Employee_Smry_Goal_Rating_Id,
Smry_Goal_Num,
Smry_Goal_Rating_Id,
Employee_Strength_Accomplishment_Text,
Employee_Area_of_Improvement_Text,
Employee_Additional_Comment_Text,
Strength_Accomplishment_Text,
Areas_of_Improvement_Text,
Additional_Comment_Text,
Lawson_Company_Num,
Process_Level_Code,
Source_System_Key,
Valid_To_Date,
Source_System_Code,
DW_Last_Update_Date_Time

)
SELECT 
    ROW_NUMBER() OVER (ORDER BY Evaluation_Record_ID) AS Employee_Performance_SID,
	Employee_SID,
    Employee_Num,
	Valid_From_Date,
	Employee_Talent_Profile_SID,
	Review_Period_Id,
	Review_Year_Num,
	Review_Period_Start_Date,
	Review_Period_End_Date,
	Performance_Plan_Id,
	Evaluation_Workflow_Status_Id,
	Manager_Full_Name,
	Manager_Employee_Num,
	Employee_Performance_Num,
	Employee_Performance_Rating_Id,
	Performance_Rating_Num,
	Performance_Rating_Id,
	Employee_Smry_Competency_Num,
	Employee_Smry_Competency_Rating_Id,
	Smry_Competency_Num,
	Smry_Competency_Rating_Id,
	Employee_Smry_Goal_Num,
	Employee_Smry_Goal_Rating_Id,
	Smry_Goal_Num,
	Smry_Goal_Rating_Id,
	Employee_Strength_Accomplishment_Text,
	Employee_Area_of_Improvement_Text,
	Employee_Additional_Comment_Text,
	Strength_Accomplishment_Text,
	Areas_of_Improvement_Text,
	Additional_Comment_Text,
	Lawson_Company_Num,
	Process_Level_Code,
	Source_System_Key,
	Valid_To_Date,
	Source_System_Code,
	DW_Last_Update_Date_Time
    
    FROM EDWHR_STAGING.Employee_Performance_Detail_WRK
    
    GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,Evaluation_Record_ID;


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

 CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR_STAGING','Employee_Performance_Detail_WRK1');
 
 .IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


BT;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

UPDATE TGT
FROM EDWHR.Employee_Performance_Detail TGT,
EDWHR_Staging.Employee_Performance_Detail_WRK1 WRK
SET Valid_To_Date = CURRENT_DATE - INTERVAL '1' DAY,
DW_Last_Update_Date_Time = CURRENT_TIMESTAMP(0)

WHERE TGT.Employee_Performance_SID = WRK.Employee_Performance_SID
 AND
	    
(
COALESCE(TGT.Employee_SID, 0) NOT = COALESCE(WRK.Employee_SID, 0) OR
COALESCE(TGT.Employee_Num, 0) NOT = COALESCE(WRK.Employee_Num, 0) OR
COALESCE(TGT.Employee_Talent_Profile_SID, 0) NOT = COALESCE(WRK.Employee_Talent_Profile_SID, 0) OR
COALESCE(TGT.Review_Period_Id, 0) NOT = COALESCE(WRK.Review_Period_Id, 0) OR
COALESCE(TGT.Review_Year_Num, 0) NOT = COALESCE(WRK.Review_Year_Num, 0) OR
COALESCE(TGT.Review_Period_Start_Date, CAST('1900-01-01' AS DATE)) NOT = COALESCE(WRK.Review_Period_Start_Date, CAST('1900-01-01' AS DATE)) OR
COALESCE(TGT.Review_Period_End_Date, CAST('1900-01-01' AS DATE))NOT = COALESCE(WRK.Review_Period_End_Date, CAST('1900-01-01' AS DATE))OR
COALESCE(TGT.Performance_Plan_Id, 0) NOT = COALESCE(WRK.Performance_Plan_Id, 0) OR
COALESCE(TGT.Evaluation_Workflow_Status_Id, 0) NOT = COALESCE(WRK.Evaluation_Workflow_Status_Id, 0) OR
COALESCE(TGT.Manager_Full_Name, '') NOT = COALESCE(WRK.Manager_Full_Name, '') OR
COALESCE(TGT.Manager_Employee_Num, 0) NOT = COALESCE(WRK.Manager_Employee_Num, 0) OR
COALESCE(TGT.Employee_Performance_Num, 0) NOT = COALESCE(WRK.Employee_Performance_Num, 0) OR
COALESCE(TGT.Employee_Performance_Rating_Id, 0) NOT = COALESCE(WRK.Employee_Performance_Rating_Id, 0) OR
COALESCE(TGT.Performance_Rating_Num, 0) NOT = COALESCE(WRK.Performance_Rating_Num, 0) OR
COALESCE(TGT.Performance_Rating_Id, 0) NOT = COALESCE(WRK.Performance_Rating_Id, 0) OR
COALESCE(TGT.Employee_Smry_Competency_Num, 0) NOT = COALESCE(WRK.Employee_Smry_Competency_Num, 0) OR
COALESCE(TGT.Employee_Smry_Competency_Rating_Id, 0) NOT = COALESCE(WRK.Employee_Smry_Competency_Rating_Id, 0) OR
COALESCE(TGT.Smry_Competency_Num, 0) NOT = COALESCE(WRK.Smry_Competency_Num, 0) OR
COALESCE(TGT.Smry_Competency_Rating_Id, 0) NOT = COALESCE(WRK.Smry_Competency_Rating_Id, 0) OR
COALESCE(TGT.Employee_Smry_Goal_Num, 0) NOT = COALESCE(WRK.Employee_Smry_Goal_Num, 0) OR
COALESCE(TGT.Employee_Smry_Goal_Rating_Id, 0) NOT = COALESCE(WRK.Employee_Smry_Goal_Rating_Id, 0) OR
COALESCE(TGT.Smry_Goal_Num, 0) NOT = COALESCE(WRK.Smry_Goal_Num, 0) OR
COALESCE(TGT.Smry_Goal_Rating_Id, 0) NOT = COALESCE(WRK.Smry_Goal_Rating_Id, 0) OR
COALESCE(TGT.Employee_Strength_Accomplishment_Text, '') NOT = COALESCE(WRK.Employee_Strength_Accomplishment_Text, '') OR
COALESCE(TGT.Employee_Area_of_Improvement_Text, '') NOT = COALESCE(WRK.Employee_Area_of_Improvement_Text, '') OR
COALESCE(TGT.Employee_Additional_Comment_Text, '') NOT = COALESCE(WRK.Employee_Additional_Comment_Text, '') OR
COALESCE(TGT.Strength_Accomplishment_Text, '') NOT = COALESCE(WRK.Strength_Accomplishment_Text, '') OR
COALESCE(TGT.Areas_of_Improvement_Text, '') NOT = COALESCE(WRK.Areas_of_Improvement_Text, '') OR
COALESCE(TGT.Additional_Comment_Text, '') NOT = COALESCE(WRK.Additional_Comment_Text, '') OR
COALESCE(TGT.Lawson_Company_Num, 0) NOT = COALESCE(WRK.Lawson_Company_Num, 0) OR
COALESCE(TGT.Process_Level_Code, '') NOT = COALESCE(WRK.Process_Level_Code, '') OR
COALESCE(TGT.Source_System_Key, '') NOT = COALESCE(WRK.Source_System_Key, '')
)
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT INTO EDWHR.Employee_Performance_Detail
(
Employee_Performance_SID,
 Employee_SID,
 Employee_Num,
Valid_From_Date,
Employee_Talent_Profile_SID,
Review_Period_Id,
Review_Year_Num,
Review_Period_Start_Date,
Review_Period_End_Date,
Performance_Plan_Id,
Evaluation_Workflow_Status_Id,
Manager_Full_Name,
Manager_Employee_Num,
Employee_Performance_Num,
Employee_Performance_Rating_Id,
Performance_Rating_Num,
Performance_Rating_Id,
Employee_Smry_Competency_Num,
Employee_Smry_Competency_Rating_Id,
Smry_Competency_Num,
Smry_Competency_Rating_Id,
Employee_Smry_Goal_Num,
Employee_Smry_Goal_Rating_Id,
Smry_Goal_Num,
Smry_Goal_Rating_Id,
Employee_Strength_Accomplishment_Text,
Employee_Area_of_Improvement_Text,
Employee_Additional_Comment_Text,
Strength_Accomplishment_Text,
Areas_of_Improvement_Text,
Additional_Comment_Text,
Lawson_Company_Num,
Process_Level_Code,
Source_System_Key,
Valid_To_Date,
Source_System_Code,
DW_Last_Update_Date_Time
)
SELECT 
WRK.Employee_Performance_SID,
 WRK.Employee_SID,
 WRK.Employee_Num,
WRK.Valid_From_Date,
WRK.Employee_Talent_Profile_SID,
WRK.Review_Period_Id,
WRK.Review_Year_Num,
WRK.Review_Period_Start_Date,
WRK.Review_Period_End_Date,
WRK.Performance_Plan_Id,
WRK.Evaluation_Workflow_Status_Id,
WRK.Manager_Full_Name,
WRK.Manager_Employee_Num,
WRK.Employee_Performance_Num,
WRK.Employee_Performance_Rating_Id,
WRK.Performance_Rating_Num,
WRK.Performance_Rating_Id,
WRK.Employee_Smry_Competency_Num,
WRK.Employee_Smry_Competency_Rating_Id,
WRK.Smry_Competency_Num,
WRK.Smry_Competency_Rating_Id,
WRK.Employee_Smry_Goal_Num,
WRK.Employee_Smry_Goal_Rating_Id,
WRK.Smry_Goal_Num,
WRK.Smry_Goal_Rating_Id,
WRK.Employee_Strength_Accomplishment_Text,
WRK.Employee_Area_of_Improvement_Text,
WRK.Employee_Additional_Comment_Text,
WRK.Strength_Accomplishment_Text,
WRK.Areas_of_Improvement_Text,
WRK.Additional_Comment_Text,
WRK.Lawson_Company_Num,
WRK.Process_Level_Code,
WRK.Source_System_Key,
WRK.Valid_To_Date,
WRK.Source_System_Code,
WRK.DW_Last_Update_Date_Time
FROM EDWHR_Staging.Employee_Performance_Detail_WRK1 WRK 

LEFT JOIN EDWHR_BASE_VIEWS.Employee_Performance_Detail TGT
ON WRK.Employee_Performance_SID = TGT.Employee_Performance_SID
AND TGT.Valid_To_Date = CAST('9999-12-31' AS DATE)

WHERE 
(
COALESCE(TGT.Employee_Performance_SID, '') <> COALESCE(WRK.Employee_Performance_SID, '') OR
COALESCE(TGT.Employee_Talent_Profile_SID, '') <> COALESCE(WRK.Employee_Talent_Profile_SID, '') OR
COALESCE(TGT.Employee_SID, '') <> COALESCE(WRK.Employee_SID, '') OR
COALESCE(TGT.Employee_Num, '') <> COALESCE(WRK.Employee_Num, '') OR
COALESCE(TGT.Review_Period_Id, '') <> COALESCE(WRK.Review_Period_Id, '') OR
COALESCE(TGT.Review_Year_Num, '1900') <> COALESCE(WRK.Review_Year_Num, '1900') OR
TGT.Review_Period_Start_Date <> WRK.Review_Period_Start_Date OR
TGT.Review_Period_End_Date <> WRK.Review_Period_End_Date OR
COALESCE(TGT.Performance_Plan_Id, '') <> COALESCE(WRK.Performance_Plan_Id, '') OR
COALESCE(TGT.Evaluation_Workflow_Status_Id, '') <> COALESCE(WRK.Evaluation_Workflow_Status_Id, '') OR
COALESCE(TGT.Manager_Full_Name, '') <> COALESCE(WRK.Manager_Full_Name, '') OR
COALESCE(TGT.Manager_Employee_Num, '') <> COALESCE(WRK.Manager_Employee_Num, '') OR
COALESCE(TGT.Employee_Performance_Num, '') <> COALESCE(WRK.Employee_Performance_Num, '') OR
COALESCE(TGT.Employee_Performance_Rating_Id, '') <> COALESCE(WRK.Employee_Performance_Rating_Id, '') OR
COALESCE(TGT.Performance_Rating_Num, '') <> COALESCE(WRK.Performance_Rating_Num, '') OR
COALESCE(TGT.Performance_Rating_Id, '') <> COALESCE(WRK.Performance_Rating_Id, '') OR
COALESCE(TGT.Employee_Smry_Competency_Num, '') <> COALESCE(WRK.Employee_Smry_Competency_Num, '') OR
COALESCE(TGT.Employee_Smry_Competency_Rating_Id, '') <> COALESCE(WRK.Employee_Smry_Competency_Rating_Id, '') OR
COALESCE(TGT.Smry_Competency_Num, '') <> COALESCE(WRK.Smry_Competency_Num, '') OR
COALESCE(TGT.Smry_Competency_Rating_Id, '') <> COALESCE(WRK.Smry_Competency_Rating_Id, '') OR
COALESCE(TGT.Employee_Smry_Goal_Num, '') <> COALESCE(WRK.Employee_Smry_Goal_Num, '') OR
COALESCE(TGT.Employee_Smry_Goal_Rating_Id, '') <> COALESCE(WRK.Employee_Smry_Goal_Rating_Id, '') OR
COALESCE(TGT.Smry_Goal_Num, '') <> COALESCE(WRK.Smry_Goal_Num, '') OR
COALESCE(TGT.Smry_Goal_Rating_Id, '') <> COALESCE(WRK.Smry_Goal_Rating_Id, '') OR
COALESCE(TGT.Employee_Strength_Accomplishment_Text, '') <> COALESCE(WRK.Employee_Strength_Accomplishment_Text, '') OR
COALESCE(TGT.Employee_Area_of_Improvement_Text, '') <> COALESCE(WRK.Employee_Area_of_Improvement_Text, '') OR
COALESCE(TGT.Employee_Additional_Comment_Text, '') <> COALESCE(WRK.Employee_Additional_Comment_Text, '') OR
COALESCE(TGT.Strength_Accomplishment_Text, '') <> COALESCE(WRK.Strength_Accomplishment_Text, '') OR
COALESCE(TGT.Areas_of_Improvement_Text, '') <> COALESCE(WRK.Areas_of_Improvement_Text, '') OR
COALESCE(TGT.Additional_Comment_Text, '') <> COALESCE(WRK.Additional_Comment_Text, '') OR
COALESCE(TGT.Lawson_Company_Num, '') <> COALESCE(WRK.Lawson_Company_Num, '') OR
COALESCE(TGT.Process_Level_Code, '') <> COALESCE(WRK.Process_Level_Code, '') OR
COALESCE(TGT.Source_System_Key, '') <> COALESCE(WRK.Source_System_Key, '')
)
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

UPDATE TGT
FROM EDWHR.Employee_Performance_Detail TGT
SET Valid_To_Date = current_date - INTERVAL '1' DAY,
 DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)
 WHERE TGT.Valid_To_Date = DATE '9999-12-31'
AND 
TGT.Employee_Performance_SID NOT IN (SELECT Employee_Performance_SID FROM EDWHR_Staging.Employee_Performance_Detail_WRK1)
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


ET;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Employee_Performance_Detail');

.QUIT;

.EXIT