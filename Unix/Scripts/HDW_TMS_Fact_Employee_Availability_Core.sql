############################################################################################
#   SCRIPT NAME     - HDW_TMS_Fact_Employee_Availability_Core.sql                 	   #
#   Job NAME        - J_HDW_TMS_Fact_Employee_Availability                       	   #
#   TARGET TABLE    - EDWHR.Fact_Employee_Availability                			   #
#   Developer   	- Heather Thacker                                  		   #
#   Version 		- 1.0 - Initial RELEASE                     			   #
#   Description 	- The SCRIPT loads the Core Table by updating, inserting,	   #
#				or retiring records based on the staging work table.	   #
#											   #
############################################################################################

bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDM_TMS_Fact_Employee_Availability ;' FOR SESSION;

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR_STAGING','Fact_Employee_Availability_Wrk');
 
.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

BT;

UPDATE TGT
FROM 
	EDWHR.Fact_Employee_Availability TGT,
	EDWHR_STAGING.Fact_Employee_Availability_Wrk STG
SET 
	VALID_TO_DATE = current_date - INTERVAL '1' DAY ,
	DW_LAST_UPDATE_DATE_TIME = STG.DW_LAST_UPDATE_DATE_TIME
WHERE
	TGT.Employee_Talent_Profile_SID = STG.Employee_Talent_Profile_SID 
	AND TGT.VALID_TO_DATE = '9999-12-31'

	AND
	(
	TRIM(COALESCE(CAST(STG.Employee_SID AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_SID AS INTEGER) ,-999))
	OR TRIM(COALESCE(CAST(STG.Employee_Num AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Num AS INTEGER) ,-999)) 
      	OR TRIM(COALESCE(CAST(STG.Jobs_Pooled_For_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Jobs_Pooled_For_Cnt AS INTEGER) ,-999))
	OR TRIM(COALESCE(CAST(STG.Employee_Talent_Pool_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Talent_Pool_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Employee_Successor_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Successor_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Employee_Ready_Now_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Ready_Now_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Employee_Ready_18_24_Month_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Ready_18_24_Month_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Employee_Ready_12_18_Month_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Ready_12_18_Month_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Employee_Ready_6_11_Month_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Ready_6_11_Month_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Employee_Other_Readiness_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Other_Readiness_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Employee_Readiness_Unknown_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Readiness_Unknown_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Empl_Slated_For_Position_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Slated_For_Position_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Emp_Talent_Pooled_For_Pos_Cnt AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Employee_Talent_Pooled_For_Position_Cnt AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Lawson_Company_Num AS INTEGER) ,-999)) <> TRIM(COALESCE(CAST(TGT.Lawson_Company_Num AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Process_Level_Code AS CHAR(5)) ,'')) <> TRIM(COALESCE(CAST(TGT.Process_Level_Code AS CHAR(5)) ,'')) 
	);


.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

INSERT INTO EDWHR.Fact_Employee_Availability
(
	Employee_Talent_Profile_SID,
	Valid_From_Date,
	Employee_SID,
 	Employee_Num,
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
	Employee_Slated_For_Position_Cnt,
	Employee_Talent_Pooled_For_Position_Cnt,
	Lawson_Company_Num,
	Process_Level_Code,
	Source_System_Code,
	DW_Last_Update_Date_Time
)
SELECT 
      STG.Employee_Talent_Profile_SID,
      Current_Date AS Valid_From_Date,
	STG.Employee_SID,
 	STG.Employee_Num,
      '9999-12-31' AS Valid_To_Date,
      STG.Jobs_Pooled_For_Cnt,
      STG.Employee_Talent_Pool_Cnt,
      STG.Employee_Successor_Cnt,
      STG.Employee_Ready_Now_Cnt,
      STG.Employee_Ready_18_24_Month_Cnt,
      STG.Employee_Ready_12_18_Month_Cnt,
      STG.Employee_Ready_6_11_Month_Cnt,
      STG.Employee_Other_Readiness_Cnt,
      STG.Employee_Readiness_Unknown_Cnt,
      STG.Empl_Slated_For_Position_Cnt,
      STG.Emp_Talent_Pooled_For_Pos_Cnt,
      STG.Lawson_Company_Num,
      STG.Process_Level_Code,
      STG.Source_System_Code,
      Current_Timestamp(0) As DW_Last_Update_Date_Time

FROM EDWHR_STAGING.Fact_Employee_Availability_Wrk STG
WHERE
(
	STG.Employee_SID,
 	STG.Employee_Num,      
	TRIM(STG.Employee_Talent_Profile_SID),
      TRIM(STG.Jobs_Pooled_For_Cnt),
      TRIM(STG.Employee_Talent_Pool_Cnt),
      TRIM(STG.Employee_Successor_Cnt),
      TRIM(STG.Employee_Ready_Now_Cnt),
      TRIM(STG.Employee_Ready_18_24_Month_Cnt),
      TRIM(STG.Employee_Ready_12_18_Month_Cnt),
      TRIM(STG.Employee_Ready_6_11_Month_Cnt),
      TRIM(STG.Employee_Other_Readiness_Cnt),
      TRIM(STG.Employee_Readiness_Unknown_Cnt),
      TRIM(STG.Empl_Slated_For_Position_Cnt),
      TRIM(STG.Emp_Talent_Pooled_For_Pos_Cnt),
      TRIM(STG.Lawson_Company_Num),
      TRIM(STG.Process_Level_Code),
      TRIM(STG.Source_System_Code)
)
NOT IN
(
SELECT
	TGT.Employee_SID,
 	TGT.Employee_Num,
      TRIM(TGT.Employee_Talent_Profile_SID),
      TRIM(TGT.Jobs_Pooled_For_Cnt),
      TRIM(TGT.Employee_Talent_Pool_Cnt),
      TRIM(TGT.Employee_Successor_Cnt),
      TRIM(TGT.Employee_Ready_Now_Cnt),
      TRIM(TGT.Employee_Ready_18_24_Month_Cnt),
      TRIM(TGT.Employee_Ready_12_18_Month_Cnt),
      TRIM(TGT.Employee_Ready_6_11_Month_Cnt),
      TRIM(TGT.Employee_Other_Readiness_Cnt),
      TRIM(TGT.Employee_Readiness_Unknown_Cnt),
      TRIM(TGT.Employee_Slated_For_Position_Cnt),
      TRIM(TGT.Employee_Talent_Pooled_For_Position_Cnt),
      TRIM(TGT.Lawson_Company_Num),
      TRIM(TGT.Process_Level_Code),
      TRIM(TGT.Source_System_Code)
FROM EDWHR_BASE_VIEWS.Fact_Employee_Availability TGT WHERE TGT.Valid_To_Date='9999-12-31'
);


.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

/*  RETIRE RECORD ON 2ND RETIRE LOGIC */

UPDATE TGT

	FROM EDWHR.Fact_Employee_Availability TGT

SET VALID_TO_DATE = current_date - INTERVAL '1' DAY ,
	DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)

WHERE 
	TGT.VALID_TO_DATE = DATE '9999-12-31'
	AND  
	(
	TGT.Employee_Talent_Profile_SID
	)
NOT IN 
	(
	SELECT Employee_Talent_Profile_SID
	FROM EDWHR_STAGING.Fact_Employee_Availability_Wrk
	GROUP BY 1
	);

ET;

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Fact_Employee_Availability');
 
.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

.Logoff;

.exit

EOF