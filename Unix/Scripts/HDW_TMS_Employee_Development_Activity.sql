#####################################################################################
#                                                                                   #
#   SCRIPT NAME     - HDW_TMS_Employee_Development_Activity.sql                          #
#   Job NAME        - J_HDW_TMS_Employee_Development_Activity                                #
#   TARGET TABLE    - EDWHR.Employee_Development_Activity                       #
#   Developer       - Julia Kim
		                                                           #
#   Version         - 1.0 - Initial RELEASE                                         #
#   Description     - The SCRIPT loads the TARGET TABLE WITH NEW records            #
#                     AND maintain their version AS TYPE 2                          #
#                                                                                   #
#####################################################################################


bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Employee_Development_Activity;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/* Collecting Stats on look-up table */

/*CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_SK_Xwlk');*/
CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','Development_Activities_Report','Trim(Development_Activity_Record_ID)', 'Employee_Development_Activity');


DELETE FROM EDWHR_STAGING.Development_Activities_Report_REJECT
;

INSERT INTO Edwhr_Staging.Development_Activities_Report_REJECT
SELECT
      STG.Employee_ID,   
      STG.Activity_Competency_Name ,
      STG.Description ,
      STG.Catalog_Activity_Name ,
      STG.Catalog_Activity_Description ,
      STG.Priority ,
      STG.Status ,
      STG.Start_Date,
      STG. End_Date ,
      STG.Hours ,
      STG.Development_Goals_Notes ,
      STG.Development_Activity_Record_ID ,
      CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,
     
     (CASE WHEN Trim(STG.Employee_ID) IS  NULL
	    THEN 'Employee_ID is NULL'
	    WHEN Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    THEN  'Employee_Id is alpha_numeric'
    	    ELSE 0 END )
                      AS REJECT_REASON,
     'Development_Activities_Report' AS REJECT_STG_TBL_NM
     
   FROM Edwhr_Staging.Development_Activities_Report STG
   where 
	     Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    
             OR  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
         AND CAST(Trim(Employee_ID) AS INT)  NOT IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee));



.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;



DELETE FROM EDWHR_Staging.Employee_Development_Activity_Wrk;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT INTO EDWHR_Staging.Employee_Development_Activity_Wrk
(
      Employee_Development_Activity_SID,  
	  Employee_Num,
	  Employee_SID,
      Employee_Talent_Profile_SID ,
      Development_Activity_Name ,
      Development_Activity_Desc ,
      Catalog_Activity_Name ,
      Catalog_Activity_Desc ,
      Development_Activity_Status_Id ,
      Development_Activity_Priority_Id ,
      Development_Activity_Start_Date ,
      Development_Activity_End_Date ,
      Development_Activity_Hour_Text ,
      Development_Activity_Comment_Text ,
      Lawson_Company_Num  ,
      Process_Level_Code  ,
      Source_System_Key ,
      Source_System_Code ,
      DW_Last_Update_Date_Time 
)
SELECT
XWLK.SK AS  Employee_Development_Activity_SID,
   Cast(STG.Employee_ID as Integer) as Employee_Num ,
    emp.Employee_SID as Employee_SID,
ETP.Employee_Talent_Profile_SID AS Employee_Talent_Profile_SID, 

STG.Activity_Competency_Name AS Activity_Competency_Name, 
STG.Description AS Description,
STG.Catalog_Activity_Name AS Catalog_Activity_Name ,
STG.Catalog_Activity_Description AS Catalog_Activity_Description,
RPS.Performance_Status_Id AS Performance_Status_Id ,
RPP.Probability_Potential_Id AS Probability_Potential_Id,
STG.Start_Date AS Start_Date,
STG.End_Date AS End_Date,
STG.Hours AS Hours,
STG.Development_Goals_Notes AS Development_Goals_Notes,
ETP.Lawson_Company_Num AS Lawson_Company_Num,
Coalesce(ETP.Process_Level_Code,'00000') AS Process_Level_Code,
STG.Development_Activity_Record_ID AS Development_Activity_Record_ID,
'M' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM
Edwhr_Staging.Development_Activities_Report STG

INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
ON trim(STG.Development_Activity_Record_ID ) = XWLK.SK_Source_Txt AND XWLK.SK_Type = 'Employee_Development_Activity'

	INNER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN Cast(Trim(STG.Employee_ID) AS INT) ELSE 0 END AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31'
	AND EMP.Lawson_Company_Num = Cast(Substr(STG.Job_Code,1,4) AS INTEGER)
    
LEFT OUTER JOIN   EDWHR_BASE_VIEWS.Employee_Talent_Profile ETP
ON (CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN Cast(Trim(STG.Employee_ID) AS INT) ELSE 0 END) = Trim(ETP.Employee_num)
	AND ETP.Valid_To_Date='9999-12-31'

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Performance_Status RPS
ON  RPS.Performance_Status_Desc = STG.Status

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Probability_Potential RPP
ON  RPP.Probability_Potential_Desc = STG.Priority
qualify row_number() over (partition by Employee_Development_Activity_SID  order by Employee_Development_Activity_SID )=1
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/* Collecting Stats on work table/s */

CALL dbadmin_procs.collect_stats_table ('EDWHR_STAGING','Employee_Development_Activity_Wrk');

BT;



UPDATE TGT
FROM EDWHR.Employee_Development_Activity TGT,
(select * from  EDWHR_Staging.Employee_Development_Activity_Wrk) WRK
SET  Valid_To_Date = Current_Date  - INTERVAL '1' DAY,
			 DW_Last_Update_Date_Time = Current_Timestamp(0)
WHERE 
			Trim(TGT.Employee_Development_Activity_SID) = Trim(WRK.Employee_Development_Activity_SID)
AND (
			Trim(Coalesce(TGT.Employee_Talent_Profile_SID,0)) <>Trim(Coalesce(WRK.Employee_Talent_Profile_SID,0))
			OR TRIM(COALESCE(CAST(TGT.Development_Activity_Name AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Development_Activity_Name AS VARCHAR(255)),''))
			OR Trim(Coalesce(TGT.Employee_Num,0)) <>Trim(Coalesce(WRK.Employee_Num,0))
			OR Trim(Coalesce(TGT.Employee_SID,0)) <>Trim(Coalesce(WRK.Employee_SID,0))
			OR TRIM(COALESCE(CAST(TGT.Development_Activity_Desc AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Development_Activity_Desc AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Catalog_Activity_Name AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Catalog_Activity_Name AS VARCHAR(255)),''))
			OR TRIM(COALESCE(CAST(TGT.Development_Activity_Desc AS VARCHAR(255)),'')) <> TRIM(COALESCE(CAST(WRK.Development_Activity_Desc AS VARCHAR(255)),''))
			OR TRIM(Coalesce(TGT.Development_Activity_Status_Id,0)) <>TRIM(Coalesce(WRK.Development_Activity_Status_Id, 0))
			OR Trim(Coalesce(TGT.Development_Activity_Priority_Id, 0)) <>Trim(Coalesce(WRK.Development_Activity_Priority_Id, 0)) 
			OR TRIM(TGT.Development_Activity_Start_Date) <> TRIM(WRK.Development_Activity_Start_Date)
			OR TRIM(TGT.Development_Activity_End_Date)  <> TRIM(WRK.Development_Activity_End_Date )
			OR TRIM(COALESCE(CAST(TGT.Development_Activity_Hour_Text AS VARCHAR(250)),'')) <> TRIM(COALESCE(CAST(WRK.Development_Activity_Hour_Text AS VARCHAR(250)),''))
			OR TRIM(COALESCE(CAST(TGT.Development_Activity_Comment_Text AS VARCHAR(250)),'')) <> TRIM(COALESCE(CAST(WRK.Development_Activity_Comment_Text AS VARCHAR(250)),''))
			OR Trim(Coalesce(TGT.Lawson_Company_Num,0))  <>Trim(Coalesce(WRK.Lawson_Company_Num, 0))  
			OR Trim(Coalesce(TGT.Process_Level_Code,''))  <>Trim(Coalesce(WRK.Process_Level_Code, '')) 
			OR TRIM(COALESCE(CAST(TGT.Source_System_Key AS VARCHAR(100)),'')) <> TRIM(COALESCE(CAST(WRK.Source_System_Key AS VARCHAR(100)),''))

			 
			)
AND TGT.Valid_TO_Date = DATE '9999-12-31'

;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;



INSERT INTO EDWHR.Employee_Development_Activity

(
      Employee_Development_Activity_SID, 
      Valid_From_Date, 
	   Employee_SID,
      Employee_Num,
      Employee_Talent_Profile_SID, 
      Development_Activity_Name, 
      Development_Activity_Desc, 
      Catalog_Activity_Name, 
      Catalog_Activity_Desc, 
      Development_Activity_Status_Id ,
      Development_Activity_Priority_Id ,
      Development_Activity_Start_Date, 
      Development_Activity_End_Date, 
      Development_Activity_Hour_Text, 
      Development_Activity_Comment_Text ,
      Lawson_Company_Num,
      Process_Level_Code, 
      Valid_To_Date,
      Source_System_Key, 
      Source_System_Code, 
      DW_Last_Update_Date_Time
 )
    
    SELECT 
      WRK.Employee_Development_Activity_SID, 
      Current_Date AS Valid_From_Date, 
	   WRK.Employee_SID,
       WRK.Employee_Num,
      WRK.Employee_Talent_Profile_SID, 
      WRK.Development_Activity_Name, 
      WRK.Development_Activity_Desc, 
      WRK.Catalog_Activity_Name, 
      WRK. Catalog_Activity_Desc, 
      WRK.Development_Activity_Status_Id ,
      WRK.Development_Activity_Priority_Id ,
      WRK.Development_Activity_Start_Date, 
      WRK.Development_Activity_End_Date, 
      WRK.Development_Activity_Hour_Text, 
      WRK.Development_Activity_Comment_Text ,
      WRK.Lawson_Company_Num,
      WRK.Process_Level_Code, 
      '9999-12-31' AS Valid_To_Date,
      WRK.Source_System_Key, 
      WRK.Source_System_Code, 
      Current_Timestamp(0) As DW_Last_Update_Date_Time
      FROM EDWHR_Staging.Employee_Development_Activity_Wrk WRK

      WHERE
      (


      WRK.Employee_Development_Activity_SID, 
	  WRK.Employee_SID,
       WRK.Employee_Num,
      WRK.Employee_Talent_Profile_SID, 
      WRK.Development_Activity_Name, 
      WRK.Development_Activity_Desc, 
      WRK.Catalog_Activity_Name, 
      WRK. Catalog_Activity_Desc, 
      WRK.Development_Activity_Status_Id ,
      WRK.Development_Activity_Priority_Id ,
      WRK.Development_Activity_Start_Date, 
      WRK.Development_Activity_End_Date, 
      WRK.Development_Activity_Hour_Text, 
      WRK.Development_Activity_Comment_Text ,
      WRK.Lawson_Company_Num,
      WRK.Process_Level_Code, 
      WRK.Source_System_Key, 
      WRK.Source_System_Code
      )
     NOT IN
     (
     SELECT 
      TGT.Employee_Development_Activity_SID, 
	  TGT.Employee_SID,
       TGT.Employee_Num,
      TGT.Employee_Talent_Profile_SID, 
      TGT.Development_Activity_Name, 
      TGT.Development_Activity_Desc, 
      TGT.Catalog_Activity_Name, 
      TGT. Catalog_Activity_Desc, 
      TGT.Development_Activity_Status_Id ,
      TGT.Development_Activity_Priority_Id ,
      TGT.Development_Activity_Start_Date, 
      TGT.Development_Activity_End_Date, 
      TGT.Development_Activity_Hour_Text, 
      TGT.Development_Activity_Comment_Text ,
      TGT.Lawson_Company_Num,
      TGT.Process_Level_Code, 
      TGT.Source_System_Key, 
      TGT.Source_System_Code
      FROM EDWHR_BASE_VIEWS.Employee_Development_Activity TGT WHERE TGT.Valid_To_Date = '9999-12-31')


;




.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

UPDATE EDWHR.Employee_Development_Activity TGT 
SET VALID_TO_DATE =current_date - INTERVAL '1' DAY ,
 DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)
WHERE 
TGT.VALID_TO_DATE = DATE '9999-12-31'
AND  
(
TGT.Employee_Development_Activity_SID)
NOT IN 
(
SELECT DISTINCT WRK.Employee_Development_Activity_SID FROM EDWHR_Staging.Employee_Development_Activity_Wrk WRK)
;

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;






ET;

/* End Transaction Block comment */

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Employee_Development_Activity');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;
.exit

EOF




























