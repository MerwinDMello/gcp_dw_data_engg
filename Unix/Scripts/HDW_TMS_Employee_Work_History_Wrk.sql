#################################################################################
#                                               				#
#   Script Name     - HDW_TMS_Employee_Work_History_Wrk.sql                	#
#   Job Name    	- J_HDW_TMS_Employee_Work_History                       #
#   TARGET TABLE    - EDWHR_Staging.Employee_Work_History_Stg         		#
#   Developer   	- Heather Thacker                                	#
#   Version 		- 1.0 - Initial Release                     		#
#   Description 	- The script loads the Staging table			#
#                                               				#
#################################################################################

bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET	QUERY_BAND = 'APP=HRDM_ETL; JOB=J_HDW_TMS_Employee_Work_History;' FOR SESSION;

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;


CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_SK_Xwlk');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

CALL EDWHR_Procs.SK_GEN('EDWHR_STAGING','Work_History_Report','COALESCE(TRIM(Work_History_ID),'''')', 'TMS_Employee_Work_History');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Work_History_Report');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

CALL dbadmin_procs.collect_stats_table ('EDWHR','Employee_Talent_Profile');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

CALL dbadmin_procs.collect_stats_table ('$NCR_TGT_SCHEMA','Address');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/*Insert Bad character records and the records that are left out after inner join with EDWHR.Employee_Talent_Profile into Reject*/


DELETE FROM edwhr_staging.Employee_Work_History_Reject;

INSERT INTO edwhr_staging.Employee_Work_History_Reject
SELECT
      STG.Employee_ID,
      STG.Work_History_Company_Name,
      STG.Work_History_Job_Title,
      STG.Work_History_Description,
      STG.Work_History_Start_Date,
      STG.Work_History_End_date,
      STG.Work_History_Address,
      STG.Work_History_City,
      STG.Work_History_Region,
      STG.Work_History_Country,
      STG.Work_History_Postal_Code,
      STG.Work_History_ID,
      CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,
      (CASE WHEN Trim(STG.Employee_ID) IS  NULL
	    THEN 'Employee_ID is NULL'
	    WHEN Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    THEN  'Employee_Id is alpha_numeric'
    	    ELSE 0 END )
                      AS REJECT_REASON,
     'Employee_Work_History' AS REJECT_STG_TBL_NM
     
   FROM EDWHR_STAGING.Work_History_Report STG
   where 
	Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    
        OR  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
        AND CAST(Trim(Employee_ID) AS INT)  NOT IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee));


     


.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

DELETE FROM $NCR_STG_SCHEMA.Employee_Work_History_Wrk;

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

INSERT INTO $NCR_STG_SCHEMA.Employee_Work_History_Wrk
(
	Employee_Work_History_SID,
	Valid_From_Date,
	Employee_Num,
	Employee_SID,
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
	XWLK.SK AS Employee_Work_History_SID,
	CURRENT_DATE AS Valid_From_Date,
	Cast(WHR.Employee_ID as Integer) as Employee_Num ,
	emp.Employee_SID as Employee_SID,
	ETP.Employee_Talent_Profile_SID AS Employee_Talent_Profile_SID,
	ADDR.Addr_SID AS Previous_Work_Address_SID,
	WHR.Work_History_Company_Name AS Work_History_Company_Name, 
	WHR.Work_History_Job_Title AS Work_History_Job_Title,  
	WHR.Work_History_Description AS Work_History_Description,
	CAST(WHR.Work_History_Start_Date AS DATE FORMAT 'mm/dd/yyyy') AS Work_History_Start_Date,
	CAST(WHR.Work_History_End_Date AS DATE FORMAT 'mm/dd/yyyy') AS Work_History_End_Date,
	ETP.Lawson_Company_Num AS Lawson_Company_Num,
	Coalesce(ETP.Process_Level_Code,'00000') AS Process_Level_Code,
	DATE '9999-12-31' AS Valid_To_Date,
	WHR.Work_History_ID AS Source_System_Key,
	'M' AS Source_System_Code,
	CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time
	FROM EDWHR_Staging.Work_History_Report WHR

INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
	ON trim(WHR.Work_History_ID)  = Trim(XWLK.SK_Source_Txt) 
	AND XWLK.SK_Type ='TMS_Employee_Work_History'
	
	INNER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(TRIM(WHR.Employee_Id) AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31'
    AND isnumeric(COALESCE(WHR.Employee_Id, 0))=0
AND EMP.Lawson_Company_Num = Cast(Substr(WHR.Job_Code,1,4) AS INTEGER)

LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee_Talent_Profile ETP 
	ON CAST(TRIM(WHR.Employee_Id) AS INTEGER) = ETP.Employee_Num
	AND ETP.Valid_To_Date = DATE '9999-12-31'
	AND isnumeric(COALESCE(WHR.Employee_Id, 0))=0

LEFT JOIN EDWHR_BASE_VIEWS.Address ADDR 
	ON COALESCE(TRIM(WHR.Work_History_Address),'') = COALESCE(TRIM(ADDR.Addr_Line_1_Text),'')
	AND COALESCE(TRIM(WHR.Work_History_City),'') = COALESCE(TRIM(ADDR.City_Name),'')
	AND COALESCE(TRIM(WHR.Work_History_Country),'') = COALESCE(TRIM(ADDR.Country_Code),'')
	AND COALESCE(TRIM(WHR.Work_History_Postal_Code),'') = COALESCE(TRIM(ADDR.Zip_Code),'')
	AND TRIM(ADDR.Addr_Type_Code) = 'PWR'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;


.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/*  Collect Statistics on the Stage Table */

CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Employee_Work_History_Wrk');

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

.Logoff;

.exit

EOF