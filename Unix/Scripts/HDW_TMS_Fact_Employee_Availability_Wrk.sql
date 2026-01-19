#################################################################################
#                                               				#
#   Script Name     - HDW_TMS_Employee_Work_History_Wrk.sql                	#
#   Job Name    	- J_HDW_TMS_Fact_Employee_Availability                  #
#   TARGET TABLE    - EDWHR_Staging.Fact_Employee_Availability_Wrk         	#
#   Developer   	- Heather Thacker                                	#
#   Version 		- 1.0 - Initial Release                     		#
#   Description 	- The script loads the Staging table			#
#                                               				#
#################################################################################

bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET	QUERY_BAND = 'APP=HRDM_ETL; JOB=J_HDW_TMS_Fact_Employee_Availability;' FOR SESSION;

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Employee_Info');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

CALL dbadmin_procs.collect_stats_table ('EDWHR','Employee_Talent_Profile');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/***************REJECT TABLE LOGIC********************/

DELETE FROM $NCR_STG_SCHEMA.Fact_Employee_Availability_Reject;

INSERT INTO $NCR_STG_SCHEMA.Fact_Employee_Availability_Reject
SELECT
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
        CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,
        (CASE WHEN Trim(STG.Employee_ID) IS  NULL
	    THEN 'Employee_ID is NULL'
	    WHEN Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    THEN  'Employee_Id is alpha_numeric'
    	    ELSE 0 END )
                      AS REJECT_REASON,
       'Fact_Employee_Availability' AS REJECT_STG_TBL_NM
       FROM EDWHR_STAGING.Employee_Info STG
       WHERE
	     Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	     OR length(Trim(STG.Employee_ID)) >= 13 
	     OR  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
             AND CAST(Trim(Employee_ID) AS INT)  NOT IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee));
.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/****************POPULATING WORK TABLE******************/


DELETE FROM $NCR_STG_SCHEMA.Fact_Employee_Availability_Wrk;

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

INSERT INTO EDWHR_Staging.Fact_Employee_Availability_Wrk
(
	Employee_Talent_Profile_SID,
	Valid_From_Date,
	Employee_Num,
	 Employee_SID,     
	Valid_To_Date,
	Jobs_Pooled_For_Cnt,
	Employee_Talent_Pool_Cnt,
	Employee_Successor_Cnt,
	Employee_Ready_Now_Cnt,
	Employee_Ready_18_24_Month_Cnt,
	Employee_Ready_12_18_Month_Cnt,
	Employee_Ready_6_11_Month_Cnt,
	Employee_Other_Readiness_Cnt,
	Employee_Readiness_Unknown_Cnt,
	Empl_Slated_For_Position_Cnt,
	Emp_Talent_Pooled_For_Pos_Cnt,
	Lawson_Company_Num,
	Process_Level_Code,
	Source_System_Code,
	DW_Last_Update_Date_Time
)

SELECT 
	TRIM(COALESCE(CAST(ETP.Employee_Talent_Profile_SID AS INTEGER) ,0)) AS Employee_Talent_Profile_SID,
	CURRENT_DATE AS Valid_From_Date,
	 Cast(STG.Employee_ID as Integer) as Employee_Num ,
    emp.Employee_SID as Employee_SID,
	DATE '9999-12-31' AS Valid_To_Date,
	TRIM(COALESCE(CAST(STG.Jobs_Pooled_For_Count AS INTEGER) ,0)) AS Jobs_Pooled_For_Cnt,
	TRIM(COALESCE(CAST(STG.Talent_Pool_Count AS INTEGER) ,0)) AS Employee_Talent_Pool_Cnt,
	TRIM(COALESCE(CAST(STG.Successors_Count AS INTEGER) ,0)) AS Employee_Successor_Cnt,
	TRIM(COALESCE(CAST(STG.Ready_Now AS INTEGER) ,0)) AS Employee_Ready_Now_Cnt,
	TRIM(COALESCE(CAST(STG.Ready_18_24_Months AS INTEGER) ,0)) AS Employee_Ready_18_24_Month_Cnt,
	TRIM(COALESCE(CAST(STG.Ready_12_18_Months AS INTEGER) ,0)) AS Employee_Ready_12_18_Month_Cnt,
	TRIM(COALESCE(CAST(STG.Ready_6_11_Months AS INTEGER) ,0)) AS Employee_Ready_6_11_Month_Cnt,
	TRIM(COALESCE(CAST(STG.Readiness_Others AS INTEGER) ,0)) AS Employee_Other_Readiness_Cnt,
	TRIM(COALESCE(CAST(STG.Readiness_Unknown AS INTEGER) ,0)) AS Employee_Readiness_Unknown_Cnt,
	TRIM(COALESCE(CAST(STG.Positions_Slated_For_Count AS INTEGER) ,0)) AS Empl_Slated_For_Position_Cnt,
	TRIM(COALESCE(CAST(STG.Positions_Talent_Pooled_Count AS INTEGER) ,0)) AS Emp_Talent_Pooled_For_Pos_Cnt,
	TRIM(COALESCE(CAST(ETP.Lawson_Company_Num AS CHAR(5)) ,'')) AS Lawson_Company_Num,
	TRIM(COALESCE(CAST(ETP.Process_Level_Code AS CHAR(5)) ,'00000')) AS Process_Level_Code,
	'M' AS Source_System_Code,
	CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
	FROM EDWHR_STAGING.Employee_Info STG
	
       INNER JOIN EDWHR_BASE_VIEWS.Employee_Talent_Profile ETP 
	ON CAST(COALESCE(STG.Employee_ID,'0') AS BIGINT) = COALESCE(ETP.Employee_Num,'0')
	AND ETP.Valid_To_Date='9999-12-31'
	
		LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(TRIM(STG.Employee_Id) AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31'
	AND EMP.Lawson_Company_Num = Cast(Substr(STG.Job_Code,1,4) AS INTEGER)
	
		WHERE SUBSTR(STG.Employee_ID,1,1) IN ('1','2','3','4','5','6','7','8','9','0')
	AND SUBSTR(STG.Employee_ID,5,1) IN ('1','2','3','4','5','6','7','8','9','0')
qualify row_number() over (partition by emp.Employee_SID  order by emp.Employee_SID )=1;

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/*  Collect Statistics on the Stage Table */

CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Fact_Employee_Availability_Wrk');

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

.Logoff;

.exit

EOF