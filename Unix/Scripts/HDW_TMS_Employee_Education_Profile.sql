#####################################################################################
#                                                                                   #
#   SCRIPT NAME     - HDW_TMS_Employee_Education_Profile.sql                        #
#   Job NAME        - J_HDW_TMS_Employee_Education_Profile                          #
#   TARGET TABLE    - EDWHR.Employee_Education_Profile                              #
#   Developer       - Julia Kim                                                     #
#		                                                                    #
#   Version         - 1.0 - Initial RELEASE                                         #
#   Description     - The SCRIPT loads the TARGET TABLE WITH NEW records            #
#                     AND maintain their version AS TYPE 2                          #
#                                                                                   #
#####################################################################################


bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Employee_Education_Profile;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/*Created Reject Table*/

Delete  from Edwhr_Staging.Education_History_Report_REJECT;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT INTO Edwhr_Staging.Education_History_Report_REJECT
(
Employee_ID,
School_Name,
School_Type,
Degree,
Major,
Education_Start_Date,
Education_End_Date,
Year_Graduated,
GPA,
Education_Comments,
Edu_Hist_Record_ID,
DW_Last_Update_Date_Time,
REJECT_REASON,
REJECT_STG_TBL_NM
)
SELECT
      Employee_ID 
      ,School_Name 
      ,School_Type 
      ,Degree 
     , Major 
      ,Education_Start_Date 
      ,Education_End_Date 
      ,Year_Graduated 
      ,GPA 
      ,Education_Comments 
     , Edu_Hist_Record_ID 
     
      ,CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,
     
     (CASE WHEN Trim(STG.Employee_ID) IS  NULL
	    THEN 'Employee_ID is NULL'
	    WHEN Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    THEN  'Employee_Id is alpha_numeric'
    	    ELSE 0 END )
                      AS REJECT_REASON,
     'Employee' AS REJECT_STG_TBL_NM
     
   FROM Edwhr_Staging.Education_History_Report STG
   where 
	     Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    OR STG.Employee_ID is null 
            OR  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
         AND CAST(Trim(Employee_ID) AS INT)  NOT IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee));
	        

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;




delete from Edwhr_Staging.Education_History_Report_REJECT2;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

Insert into Edwhr_Staging.Education_History_Report_REJECT2
(
Education_End_Date
)
sel Education_End_Date from Edwhr_Staging.Education_History_Report STG
where ( Substr(Trim(STG.Employee_ID),1,1)   IN ('1','2','3','4','5','6','7','8','9')
	    AND  STG.Employee_ID is not null 
            AND  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
         AND CAST(Trim(Employee_ID) AS INT)   IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee)))
  AND Education_End_Date = ' ';
 

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

del from Edwhr_Staging.Education_History_Report_REJECT3;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

insert into Edwhr_Staging.Education_History_Report_REJECT3
(
Education_End_Date
)
sel Education_End_Date from Edwhr_Staging.Education_History_Report STG
where ( Substr(Trim(STG.Employee_ID),1,1)   IN ('1','2','3','4','5','6','7','8','9')
	    AND  STG.Employee_ID is not null 
            AND  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
         AND CAST(Trim(Employee_ID) AS INT)   IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee)))
  AND Education_Start_Date = ' ';
 

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;



del from  Edwhr_Staging.Additional_Education_History_REJECT;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

Insert Into Edwhr_Staging.Additional_Education_History_REJECT
(
Employee_ID,
Spoken_Languages,
Written_Languages,
Prof_Org_Free_Text,
Licenses_Certifications,
Skills_Experience,
Special_Training,
Passionate_Job_Functions,
Successful_Job_Functions,
EDU_HIST_Record_ID,
DW_Last_Update_Date_Time,
REJECT_REASON,
REJECT_STG_TBL_NM
)
SELECT
      Employee_ID 
      ,Spoken_Languages 
      ,Written_Languages 
      ,Prof_Org_Free_Text 
     , Licenses_Certifications 
      ,Skills_Experience 
      ,Special_Training 
      ,Passionate_Job_Functions 
      ,Successful_Job_Functions      
     , Edu_Hist_Record_ID      
      ,CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time,
     
     (CASE WHEN Trim(STG.Employee_ID) IS  NULL
	    THEN 'Employee_ID is NULL'
	    WHEN Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    THEN  'Employee_Id is alpha_numeric'
    	    ELSE 0 END )
                      AS REJECT_REASON,
     'Employee' AS REJECT_STG_TBL_NM
     
   FROM Edwhr_Staging.Additional_Education_History STG
   where 
	     Substr(Trim(STG.Employee_ID),1,1)  NOT IN ('1','2','3','4','5','6','7','8','9')
	    OR STG.Employee_ID is null 
            OR  ( Substr(Trim(STG.Employee_ID),1,1)  IN ('1','2','3','4','5','6','7','8','9')
         AND CAST(Trim(Employee_ID) AS INT)  NOT IN (sel Employee_Num from EDWHR_BASE_VIEWS.Employee))
       ;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE; 



/* Collecting Stats on look-up table */



CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','Education_History_Report ','Trim(Edu_Hist_Record_ID)', 'Employee_Education_Profile');
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DELETE FROM EDWHR_STAGING.Employee_Education_Profile_Wrk2;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT INTO EDWHR_STAGING.Employee_Education_Profile_Wrk2
(
Employee_Education_Profile_SID
,Employee_Num
,Employee_SID
,Employee_Talent_Profile_SID 
,Employee_Education_Type_Code
,Detail_Value_Alpahnumeric_Text
,Detail_Value_Num
,Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,Source_System_Code
,DW_Last_Update_Date_Time
)
SELECT
 XWLK.SK AS Employee_Education_Profile_SID
    ,Cast(STG.Employee_ID as Integer) as Employee_Num 
   , emp.Employee_SID as Employee_SID
,ETP.Employee_Talent_Profile_SID
,'0' AS Employee_Education_Type_Code
,'0' AS Detail_Value_Alpahnumeric_Text
,0 AS Detail_Value_Num
,Current_Date Detail_Value_Date
,Trim(Coalesce(ETP.Lawson_Company_Num, 0)) AS Lawson_Company_Num
,Trim(coalesce(ETP.Process_Level_Code, 0)) AS Process_Level_Code
,STG.Edu_Hist_Record_ID AS Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIM

FROM edwhr_staging.Education_History_Report STG

INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
ON trim(STG.Edu_Hist_Record_ID) = XWLK.SK_Source_Txt AND XWLK.SK_Type = 'Employee_Education_Profile'

INNER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG.Employee_ID) AS INT) ELSE 0 END AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31'
	AND EMP.Lawson_Company_Num = Cast(Substr(STG.Job_Code,1,4) AS INTEGER)

LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee_Talent_Profile ETP
ON (CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG.Employee_ID) AS INT) ELSE 0 END )= Trim(ETP.Employee_num)
	AND ETP.Valid_To_Date='9999-12-31'


LEFT OUTER JOIN (sel distinct Employee_Id from edwhr_staging.Additional_Education_History) STG2
ON (

(CASE WHEN Trim(STG2.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG2.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG2.Employee_ID) AS INT) ELSE 0 END )= Trim(ETP.Employee_num)
)
; 

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/* Collecting Stats on work table/s */

CALL dbadmin_procs.collect_stats_table ('EDWHR_STAGING','Employee_Education_Profile_Wrk2');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*Create a new table for the values from  edwhr_staging.Education_History_Report*/


DROP TABLE EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

create multiset table EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk as (SELECT
 XWLK.SK AS Employee_Education_Profile_SID
     ,Cast(STG.Employee_ID as Integer) as Employee_Num 
   , emp.Employee_SID as Employee_SID
,ETP.Employee_Talent_Profile_SID
,School_Name
,School_Type
,Degree
,Major
,Education_Start_Date
,Education_End_Date
,Year_Graduated
,GPA
,Education_Comments
,Current_Date Detail_Value_Date
,Trim(Coalesce(ETP.Lawson_Company_Num, 0)) AS Lawson_Company_Num
,Trim(coalesce(ETP.Process_Level_Code, '00000')) AS Process_Level_Code
,STG.Edu_Hist_Record_ID AS Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIM

FROM edwhr_staging.Education_History_Report STG

INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
ON trim(STG.Edu_Hist_Record_ID) = XWLK.SK_Source_Txt AND XWLK.SK_Type = 'Employee_Education_Profile'


INNER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG.Employee_ID) AS INT) ELSE 0 END AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31'
	AND EMP.Lawson_Company_Num = Cast(Substr(STG.Job_Code,1,4) AS INTEGER)

LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee_Talent_Profile ETP
ON (CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG.Employee_ID) AS INT) ELSE 0 END )= Trim(ETP.Employee_num)
	AND ETP.Valid_To_Date='9999-12-31'


LEFT OUTER JOIN (sel distinct Employee_Id from edwhr_staging.Additional_Education_History) STG2
ON (

(CASE WHEN Trim(STG2.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG2.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG2.Employee_ID) AS INT) ELSE 0 END )= Trim(ETP.Employee_num)
)
) with data 
;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
 /*create a table for the values from edwhr_staging.Additional_Education_History*/


---Commenting this code due to HDM-1714 as we are not loadind data into edwhr_staging.Additional_Education_History table
/*DROP TABLE EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

create multiset table EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1 as (SELECT
 XWLK.SK AS Employee_Education_Profile_SID
   ,Cast(STG.Employee_ID as Integer) as Employee_Num 
   , emp.Employee_SID as Employee_SID
,ETP.Employee_Talent_Profile_SID
,Spoken_Languages 
  ,Written_Languages 
     , Prof_Org_Free_Text 
     , Licenses_Certifications
      ,Skills_Experience 
      ,Special_Training 
     , Passionate_Job_Functions 
      ,Successful_Job_Functions 
,'0'AS Detail_Value_Date
,Trim(Coalesce(ETP.Lawson_Company_Num, 0)) AS Lawson_Company_Num
,Trim(coalesce(ETP.Process_Level_Code, 0)) AS Process_Level_Code
,STG.Edu_Hist_Record_ID AS Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIM

FROM edwhr_staging.Additional_Education_History STG

INNER JOIN EDWHR_STAGING.Ref_SK_Xwlk XWLK
ON trim(STG.Edu_Hist_Record_ID) = XWLK.SK_Source_Txt AND XWLK.SK_Type = 'Employee_Education_Profile'

INNER JOIN EDWHR_BASE_VIEWS.Employee emp
	ON   CAST(CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG.Employee_ID) AS INT) ELSE 0 END AS INTEGER) = emp.Employee_Num
	AND EMP.Valid_To_Date='9999-12-31'
	AND EMP.Lawson_Company_Num = Cast(Substr(STG.Job_Code,1,4) AS INTEGER)

LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee_Talent_Profile ETP
ON (CASE WHEN Trim(STG.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG.Employee_ID) AS INT) ELSE 0 END )= Trim(ETP.Employee_num)
	AND ETP.Valid_To_Date='9999-12-31'


LEFT OUTER JOIN (sel distinct Employee_Id from edwhr_staging.Additional_Education_History) STG2
ON (

(CASE WHEN Trim(STG2.Employee_ID) IS NOT NULL  AND
	Substr(Trim(STG2.Employee_ID),1,1) IN ('1','2','3','4','5','6','7','8','9')
          THEN CAST(Trim(STG2.Employee_ID) AS INT) ELSE 0 END )= Trim(ETP.Employee_num)
)
) with data 

;*/
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DELETE FROM EDWHR_STAGING.Employee_Education_Profile_Wrk3;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT INTO EDWHR_STAGING.Employee_Education_Profile_Wrk3
(
Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID 
,Employee_Education_Type_Code
,Detail_Value_Alpahnumeric_Text
, Detail_Value_Num
, Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,SOURCE_SYSTEM_CODE
,DW_LAST_UPDATE_DATE_TIME
)
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,CAST('School Name'AS VARCHAR(50)) 
,CASE WHEN CAST(School_Name AS VARCHAR(250)) IS NOT NULL THEN CAST(School_Name AS VARCHAR(250)) 
 WHEN CAST(School_Name AS VARCHAR(250)) <> '' THEN CAST(School_Name AS VARCHAR(250))  END AS School_Name
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,CAST(Source_System_Key AS VARCHAR(100)) AS Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
where School_Name is not null
UNION ALL
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'School Type'
,CASE WHEN School_Type IS NOT NULL THEN  School_Type 
				WHEN School_Type <> '' THEN School_Type END AS School_Type
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
where school_type is not null
or school_type <> ''
UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Degree'
,Degree
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
where degree is not null 
UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Major'
,Major
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
where major is not null 
UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Education Start Date'
, ''
,0 AS Detail_Value_Nu
,CASE WHEN Education_End_Date IS NUll THEN Null ELSE CAST(Education_Start_Date as date format 'mm/dd/yyyy') END  AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
where Education_Start_Date <> ''
UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Education End Date'
, ''
,0 AS Detail_Value_Num
,CASE WHEN Education_End_Date IS NUll THEN Null ELSE CAST(Education_End_Date as date format 'mm/dd/yyyy') END  AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
 from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
 where education_end_date <> ''

UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Year Graduated'
,Year_Graduated
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
where year_graduated is not null
UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'GPA'
,GPA
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
where gpa is not null
UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Education Comments'
,Education_Comments
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk 
where education_comments is not null

---Commenting this code due to HDM-1714 
/*UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Spoken Languages'
,Spoken_Languages
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1
where spoken_languages is not null
UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Written Languages'
,Written_Languages
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1
where written_languages is not null

UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Professional Organizations Free Text'
,Prof_Org_Free_Text
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1
where Prof_Org_Free_Text is not null
UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Licenses Certifications'
,Licenses_Certifications
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1
where Licenses_Certifications is not null

UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Skills Experience'
,Skills_Experience
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1
where Skills_Experience is not null

UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Special Training'
,Special_Training
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1
where Special_Training is not null 
or Special_Training <> ''

UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Passionate Job Functions'
,Passionate_Job_Functions
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1
where Passionate_Job_Functions is not null

UNION ALL 
sel  Employee_Education_Profile_SID
,Employee_SID
 ,Employee_Num
,Employee_Talent_Profile_SID
,'Successful Job Functions'
,Successful_Job_Functions
,0 AS Detail_Value_Num
,null AS Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
,Source_System_Key
,'M' AS SOURCE_SYSTEM_CODE
,CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
from EDWHR_Staging.Employee_Edu_Hist_Rpt_Wrk1
where Successful_Job_Functions is not null 

*/
;



.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DELETE FROM EDWHR_Staging.Employee_Education_Profile_Wrk;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT INTO EDWHR_Staging.Employee_Education_Profile_Wrk
select * from EDWHR_Staging.Employee_Education_Profile_Wrk3
qualify row_number() over (partition by Employee_Education_Profile_SID, Employee_Education_Type_Code, Employee_Talent_Profile_SID order by Employee_Education_Profile_SID) = 1
;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;



/* Collecting Stats on work table/s */

CALL dbadmin_procs.collect_stats_table ('EDWHR_STAGING','Employee_Education_Profile_Wrk');

BT;


/* To Retire a record - Match on the keys and non-key fileds have changed*/

UPDATE TGT
FROM  EDWHR.Employee_Education_Profile TGT,
(select * from EDWHR_Staging.Employee_Education_Profile_Wrk) WRK
SET  Valid_To_Date = Current_Date  - INTERVAL '1' DAY,
			 DW_Last_Update_Date_Time = Current_Timestamp(0)
			 
WHERE  Trim(TGT.Employee_Education_Profile_SID) = Trim(WRK.Employee_Education_Profile_SID)
AND Trim(Coalesce(TGT.Employee_Education_Type_Code,'')) = Trim(Coalesce(WRK.Employee_Education_Type_Code,''))
AND (Trim(Coalesce(TGT.Employee_Talent_Profile_SID,0)) <>Trim(Coalesce(WRK.Employee_Talent_Profile_SID,0))
		OR Trim(Coalesce(TGT.Employee_SID,0)) <>Trim(Coalesce(WRK.Employee_SID,0))
		OR Trim(Coalesce(TGT.Employee_Num,0)) <>Trim(Coalesce(WRK.Employee_Num,0))
			OR Trim(Coalesce(TGT.Detail_Value_Alpahnumeric_Text,'')) <>Trim(Coalesce(WRK.Detail_Value_Alpahnumeric_Text,''))
			OR Trim(Coalesce(TGT.Detail_Value_Num,0)) <>Trim(Coalesce(WRK.Detail_Value_Num,0))
			OR Trim(Coalesce(TGT.Detail_Value_Date, null )) <>Trim(Coalesce(WRK.Detail_Value_Date,null))
			OR Trim(Coalesce(TGT.Lawson_Company_Num,0)) <>Trim(Coalesce(WRK.Lawson_Company_Num,0))
			OR Trim(Coalesce(TGT.Process_Level_Code,0)) <>Trim(Coalesce(WRK.Process_Level_Code,0))
			OR Trim(Coalesce(TGT.Source_System_Key,'')) <>Trim(Coalesce(WRK.Source_System_Key,''))
			OR Trim(Coalesce(TGT.SOURCE_SYSTEM_CODE,0)) <>Trim(Coalesce(WRK.SOURCE_SYSTEM_CODE,0))
			
			)
			
AND TGT.Valid_To_Date = '9999-12-31'



;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;





/*Core Table Load*/


INSERT INTO EDWHR.Employee_Education_Profile
(
  Employee_Education_Profile_SID
   ,Employee_SID
 ,Employee_Num
, Employee_Education_Type_Code
, Valid_From_Date
 ,Employee_Talent_Profile_SID 
,Detail_Value_Alpahnumeric_Text
, Detail_Value_Num
, Detail_Value_Date
,Lawson_Company_Num
,Process_Level_Code
, Valid_To_Date
,Source_System_Key
,SOURCE_SYSTEM_CODE
,DW_LAST_UPDATE_DATE_TIME
)
sel 
 WRK.Employee_Education_Profile_SID
    ,WRK.Employee_SID
 ,WRK.Employee_Num
, Trim(WRK.Employee_Education_Type_Code)
, Current_Date AS Valid_From_Date
 ,WRK.Employee_Talent_Profile_SID 
,Trim(WRK.Detail_Value_Alpahnumeric_Text)
, WRK.Detail_Value_Num
, Trim(WRK.Detail_Value_Date)
,WRK.Lawson_Company_Num
,WRK.Process_Level_Code
,'9999-12-31' AS Valid_To_Date
,WRK.Source_System_Key
,WRK.SOURCE_SYSTEM_CODE
,Current_Timestamp(0) As DW_Last_Update_Date_Time

from EDWHR_Staging.Employee_Education_Profile_Wrk WRK

WHERE 
 (
 WRK.Employee_Education_Profile_SID
    ,WRK.Employee_SID
 ,WRK.Employee_Num
, Trim(WRK.Employee_Education_Type_Code)
 ,WRK.Employee_Talent_Profile_SID 
,Trim(WRK.Detail_Value_Alpahnumeric_Text)
, WRK.Detail_Value_Num
, Trim(WRK.Detail_Value_Date)
,WRK.Lawson_Company_Num
,WRK.Process_Level_Code
,WRK.Source_System_Key
,WRK.SOURCE_SYSTEM_CODE
)

NOT IN
(Select
 TGT.Employee_Education_Profile_SID
    ,TGT.Employee_SID
 ,TGT.Employee_Num
, Trim(TGT.Employee_Education_Type_Code)
 ,TGT.Employee_Talent_Profile_SID 
,Trim(TGT.Detail_Value_Alpahnumeric_Text)
, TGT.Detail_Value_Num
, Trim(TGT.Detail_Value_Date)
,TGT.Lawson_Company_Num
,TGT.Process_Level_Code

,TGT.Source_System_Key
,TGT.SOURCE_SYSTEM_CODE
From EDWHR_BASE_VIEWS.Employee_Education_Profile TGT
WHERE TGT.Valid_To_Date = '9999-12-31'
);


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/* To retire a record-The record is no longer available in the staging table */

UPDATE EDWHR.Employee_Education_Profile TGT 
SET VALID_TO_DATE =current_date - INTERVAL '1' DAY ,
 DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)
WHERE 
TGT.VALID_TO_DATE = DATE '9999-12-31'
AND  
(
TGT.Employee_Education_Profile_SID)
NOT IN 
(
SELECT DISTINCT WRK.Employee_Education_Profile_SID FROM EDWHR_Staging.Employee_Education_Profile_Wrk WRK)
;

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;




ET;

/* End Transaction Block comment */

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Employee_Education_Profile');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;















