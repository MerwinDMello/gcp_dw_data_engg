############################################################################################
#   SCRIPT NAME     - HDW_TMS_Employee_Work_History_Core.sql                 	   	   #
#   Job NAME        - J_HDW_TMS_Employee_Work_History                        	   	   #
#   TARGET TABLE    - EDWHR.Employee_Work_History                     			   #
#   Developer   	- Heather Thacker                                  		   #
#   Version 		- 1.0 - Initial RELEASE                     			   #
#   Description 	- The SCRIPT loads the Core Table by updating, inserting,	   #
#				or retiring records based on the staging work table.	   #
#											   #
############################################################################################

bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Employee_Work_History ;' FOR SESSION;

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR_STAGING','Employee_Work_History_Wrk');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/*  Collect Statistics on the Target Table */

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Employee_Work_History');
 
.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

BT;

UPDATE TGT
FROM 
	EDWHR.Employee_Work_History TGT,
	EDWHR_STAGING.Employee_Work_History_WRK STG
SET 
	VALID_TO_DATE = current_date - INTERVAL '1' DAY ,
	DW_LAST_UPDATE_DATE_TIME = STG.DW_LAST_UPDATE_DATE_TIME
WHERE
	TGT.Employee_Work_History_SID = STG.Employee_Work_History_SID 
	AND TGT.VALID_TO_DATE = '9999-12-31'

	AND

	(
      	TRIM(COALESCE(CAST(STG.Employee_SID AS INTEGER) ,-999)) NOT= TRIM(COALESCE(CAST(STG.Employee_SID AS INTEGER) ,-999))
	OR TRIM(COALESCE(CAST(STG.Employee_Num AS INTEGER) ,-999)) NOT= TRIM(COALESCE(CAST(TGT.Employee_Num AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Employee_Talent_Profile_SID AS INTEGER) ,-999)) NOT= TRIM(COALESCE(CAST(STG.Employee_Talent_Profile_SID AS INTEGER) ,-999))
	OR TRIM(COALESCE(CAST(STG.Previous_Work_Address_SID AS INTEGER) ,-999)) NOT= TRIM(COALESCE(CAST(TGT.Previous_Work_Address_SID AS INTEGER) ,-999))
	OR TRIM(COALESCE(CAST(STG.Work_History_Company_Name AS VARCHAR(80)) ,'')) NOT= TRIM(COALESCE(CAST(TGT.Work_History_Company_Name AS VARCHAR(80)) ,'')) 
	OR TRIM(COALESCE(CAST(STG.Work_History_Job_Title_Text  AS VARCHAR(50)) ,'')) NOT= TRIM(COALESCE(CAST(TGT.Work_History_Job_Title_Text AS VARCHAR(50)) ,'')) 
	OR TRIM(COALESCE(CAST(STG.Work_History_Desc AS VARCHAR(255)) ,'')) NOT=TRIM(COALESCE(CAST(TGT.Work_History_Desc AS VARCHAR(255)) ,'')) 
	OR STG.Work_History_Start_Date NOT= TGT.Work_History_Start_Date 
	OR STG.Work_History_End_Date NOT= TGT.Work_History_End_Date 
	OR TRIM(COALESCE(CAST(STG.Lawson_Company_Num AS INTEGER) ,-999)) NOT= TRIM(COALESCE(CAST(TGT.Lawson_Company_Num AS INTEGER) ,-999)) 
	OR TRIM(COALESCE(CAST(STG.Process_Level_Code AS CHAR(5)) ,'')) NOT= TRIM(COALESCE(CAST(TGT.Process_Level_Code AS CHAR(5)) ,'')) 
	);

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;
INSERT INTO EDWHR.Employee_Work_History
(
      Employee_Work_History_SID,
      Valid_From_Date,
 	Employee_SID,
 	Employee_Num,
      Employee_Talent_Profile_SID,
      Previous_Work_Address_SID,
      Work_History_Company_Name,
      Work_History_Job_Title_Text,
      Work_History_Desc,
      Work_History_Start_Date,
      Work_History_End_Date,
      Lawson_Company_Num,
      Process_Level_Code,
      Valid_To_Date,
      Source_System_Key,
      Source_System_Code,
      DW_Last_Update_Date_Time
)
SELECT 
      STG.Employee_Work_History_SID,
      Current_Date AS Valid_From_Date,
 	STG.Employee_SID,
 	STG.Employee_Num,
      STG.Employee_Talent_Profile_SID,
      STG.Previous_Work_Address_SID,
      STG.Work_History_Company_Name,
      STG.Work_History_Job_Title_Text,
      STG.Work_History_Desc,
      STG.Work_History_Start_Date,
      STG.Work_History_End_Date,
      STG.Lawson_Company_Num,
      STG.Process_Level_Code,
      '9999-12-31' AS Valid_To_Date,
      STG.Source_System_Key,
      STG.Source_System_Code,
      Current_Timestamp(0) As DW_Last_Update_Date_Time
FROM EDWHR_STAGING.Employee_Work_History_Wrk STG
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
WHERE
(
	 STG.Employee_SID,
 	 STG.Employee_Num,      
	TRIM(STG.Employee_Talent_Profile_SID),
      TRIM(STG.Employee_Work_History_SID),
      TRIM(STG.Previous_Work_Address_SID),
      TRIM(STG.Work_History_Company_Name),
      TRIM(STG.Work_History_Job_Title_Text),
      TRIM(STG.Work_History_Desc),
      TRIM(STG.Work_History_Start_Date),
      TRIM(STG.Work_History_End_Date),
      TRIM(STG.Lawson_Company_Num),
      TRIM(STG.Process_Level_Code),
      TRIM(STG.Source_System_Key)
)
NOT IN
(
SELECT
TGT.Employee_SID,
 	 TGT.Employee_Num,
      TRIM(TGT.Employee_Talent_Profile_SID),
      TRIM(TGT.Employee_Work_History_SID),
      TRIM(TGT.Previous_Work_Address_SID),
      TRIM(TGT.Work_History_Company_Name),
      TRIM(TGT.Work_History_Job_Title_Text),
      TRIM(TGT.Work_History_Desc),
      TRIM(TGT.Work_History_Start_Date),
      TRIM(TGT.Work_History_End_Date),
      TRIM(TGT.Lawson_Company_Num),
      TRIM(TGT.Process_Level_Code),
      TRIM(TGT.Source_System_Key)
FROM EDWHR_BASE_VIEWS.Employee_Work_History TGT where TGT.Valid_To_Date='9999-12-31'
);

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

/*  RETIRE RECORD ON 2ND RETIRE LOGIC */

UPDATE TGT

	FROM 	EDWHR.Employee_Work_History TGT

SET VALID_TO_DATE = current_date - INTERVAL '1' DAY ,
	DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)

WHERE 
	TGT.VALID_TO_DATE = DATE '9999-12-31'
	AND  
	(
	TGT.Employee_Work_History_SID
	)
NOT IN 
	(
	SELECT Employee_Work_History_SID
	FROM EDWHR_STAGING.Employee_Work_History_Wrk
	GROUP BY 1
	);

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

ET;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Employee_Work_History');
 
.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

.Logoff;

.exit

EOF