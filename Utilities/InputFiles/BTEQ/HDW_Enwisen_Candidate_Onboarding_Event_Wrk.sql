############################################################################################
#                                                                                   	   #
#   Script Name - HDW_Enwisen_Candidate_Onboarding_Event_Wrk.sql                                        #
#   Job Name    - J_HDW_Enwisen_Candidate_Onboarding_Event                                             #
#   Target Table- $NCR_STG_SCHEMA.Candidate_Onboarding_Event_WRK                               	   #
#   Developer   - Syntel                                                          	   #
#   JIRA	- HDM-1834								   #
#	Initial Version		   #
#   		   					   # 
############################################################################################

bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;



SET QUERY_BAND = 'App=HR_Enwisen_ETL; Job=J_HDW_Enwisen_Candidate_Onboarding_Event;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/*  Generate the surrogate keys for Candidate_Onboarding_Event */

Delete from EDWHR_Staging.CANDIDATE_ONBOARDING_EVENT_XWLK_WRK;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

Insert into EDWHR_Staging.CANDIDATE_ONBOARDING_EVENT_XWLK_WRK
(
Requisition_Number,
Process_Level_Code ,
Data_Type,
DW_Last_Update_Date_Time
)
sel
Requisition_Number,
Process_Level_Code,
Data_Type,
DW_Last_Update_Date_Time from
(
sel distinct
Trim(Reverse(Trim(Substring(Reverse(subject) FROM 1 FOR ( Position('-' IN Reverse(subject)) - 1))))) as Requisition_Number,
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5))))) as Process_Level_Code,
'T' as Data_Type,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg
WHERE Topic = 'Z-Auto-Onboarding Confirm'
AND Category = 'Z-Auto-Onboarding Confirm'
AND SubCategory = 'Z-Auto-Onboarding Confirm'
AND InStr(Subject,'-') <> 0
AND Subject like  'Onboarding Action Required %'     
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'

Union

sel distinct
Trim(Reverse(Trim(Substring(Reverse(subject) FROM 1 FOR ( Position('-' IN Reverse(subject)) - 1))))) as Requisition_Number,
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5))))) as Process_Level_Code,
'D' as Data_Type,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg
WHERE Topic = 'Hire My Employees'
AND Category = 'Hiring Employees'
AND SubCategory = 'Drug Screen Results'
AND Subject Like  'CONFIRMED DS Results%'      
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'

Union

sel distinct
Trim(Reverse(Trim(Substring(OREPLACE(Reverse(subject),')','') FROM 1 FOR ( Position('-' IN OREPLACE(Reverse(subject),')','')) - 1))))) as Requisition_Number,
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5))))) as Process_Level_Code,
'B' as Data_Type,
Current_Timestamp(0) as DW_Last_Update_Date_Time
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg
WHERE Topic  = 'Z-Auto-BG Check Auth'
AND Category  = 'Z-Auto-BG Check Auth'
AND SubCategory = 'Z-Auto-BG Check Auth'
AND Subject like 'BG Auth Complete%'      
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'
AND InStr(Subject,'-') <> 0
) iq
where char_length(Requisition_Number)<>0
and char_length(Process_Level_Code) <> 0
and LOWER(Requisition_Number) =Upper(Requisition_Number) (CASESPECIFIC)
and LOWER(Process_Level_Code) =Upper(Process_Level_Code) (CASESPECIFIC)
and Requisition_Number  not like '%*%'
and Requisition_Number not like '%/%'
and Requisition_Number not like '%)%'
and Requisition_Number not like '%(%'
and Requisition_Number not like '%.%'
and Requisition_Number not like '%]%'
and Requisition_Number not like '%[%'
and Requisition_Number not like '% %'
and Requisition_Number not like '%:%'
and Requisition_Number not like '%''%'
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWHR_STAGING','CANDIDATE_ONBOARDING_EVENT_XWLK_WRK');


CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','CANDIDATE_ONBOARDING_EVENT_XWLK_WRK','Coalesce(Trim(Requisition_Number)||Trim(Process_Level_Code)||Trim(Data_Type),-1)','CANDIDATE_ONBOARDING_EVENT');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/*  Truncate Work Table     */

Delete from EDWHR_STAGING.Candidate_Onboarding_Event_Wrk;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/*  Load Work Table with working Data */
Insert into EDWHR_STAGING.Candidate_Onboarding_Event_Wrk
(
Candidate_Onboarding_Event_SID,
Valid_From_Date,
Event_Type_ID,
Recruitment_Requisition_Num_Text,
Completed_Date,
Candidate_SID,
Resource_Screening_Package_Num,
Sequence_Num,
Source_System_Code,
DW_Last_Update_Date_Time
)
Select
xwlk.sk as Candidate_Onboarding_Event_SID,
Current_Date as Valid_From_Date,
Cast(ROE.Event_Type_ID as Char(1))as Event_Type_ID,
Trim(Reverse(Trim(Substring(Reverse(STG.subject) FROM ( Position('-' IN Reverse(STG.subject)) + 1) FOR (5)))))||'-'||
Trim(Reverse(Trim(Substring(Reverse(STG.subject) FROM 1 FOR ( Position('-' IN Reverse(STG.subject)) - 1))))) as Recruitment_Requisition_Num_Text,
Cast(Trim(STG.CreatedDateTime) as TimeStamp(0)) as Completed_Date,
NULL AS Candidate_SID,
NULL AS Resource_Screening_Package_Num,
NULL AS Sequence_Num,
'W' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
From EDWHR_STAGING.Enwisen_CM5Tickets_Stg STG
INNER JOIN EDWHR_STAGING.Ref_SK_XWLK xwlk
On Coalesce(Trim(Reverse(Trim(Substring(Reverse(subject) FROM 1 FOR ( Position('-' IN Reverse(subject)) - 1)))))||
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5)))))||'T',-1)  = xwlk.sk_source_txt and xwlk.sk_type = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN EDWHR_Base_Views.Ref_Onboarding_Event_Type ROE
On Event_Type_Code='T'
WHERE Topic = 'Z-Auto-Onboarding Confirm'
AND Category = 'Z-Auto-Onboarding Confirm'
AND SubCategory = 'Z-Auto-Onboarding Confirm'
AND InStr(Subject,'-') <> 0
AND Subject like  'Onboarding Action Required %'     
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'
Qualify Row_Number() Over(Partition by xwlk.sk Order By STG.CreatedDateTime Desc)  = 1

Union 

sel xwlk.sk as Candidate_Onboarding_Event_SID,
Current_Date as Valid_From_Date,
Cast(ROE.Event_Type_ID as Char(1)) as Event_Type_ID,
Trim(Reverse(Trim(Substring(Reverse(STG.subject) FROM ( Position('-' IN Reverse(STG.subject)) + 1) FOR (5)))))||'-'||
Trim(Reverse(Trim(Substring(Reverse(STG.subject) FROM 1 FOR ( Position('-' IN Reverse(STG.subject)) - 1))))) as Recruitment_Requisition_Num_Text,
Cast(Trim(STG.CreatedDateTime) as TimeStamp(0)) as Completed_Date,
NULL AS Candidate_SID,
NULL AS Resource_Screening_Package_Num,
NULL AS Sequence_Num,
'W' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
From EDWHR_STAGING.Enwisen_CM5Tickets_Stg STG
INNER JOIN EDWHR_STAGING.Ref_SK_XWLK xwlk
On Coalesce(Trim(Reverse(Trim(Substring(Reverse(subject) FROM 1 FOR ( Position('-' IN Reverse(subject)) - 1)))))||
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5)))))||'D',-1)  = xwlk.sk_source_txt and xwlk.sk_type = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN EDWHR_Base_Views.Ref_Onboarding_Event_Type ROE
On Event_Type_Code='D'
WHERE Topic = 'Hire My Employees'
AND Category = 'Hiring Employees'
AND SubCategory = 'Drug Screen Results'
AND Subject Like  'CONFIRMED DS Results%'      
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'
Qualify Row_Number() Over(Partition by xwlk.sk Order By STG.CreatedDateTime Desc)  = 1

Union 

sel xwlk.sk as Candidate_Onboarding_Event_SID,
Current_Date as Valid_From_Date,
Cast(ROE.Event_Type_ID as Char(1)) as Event_Type_ID,
Trim(Reverse(Trim(Substring(Reverse(STG.subject) FROM ( Position('-' IN Reverse(STG.subject)) + 1) FOR (5)))))||'-'||
Trim(Reverse(Trim(Substring(OREPLACE(Reverse(subject),')','') FROM 1 FOR ( Position('-' IN OREPLACE(Reverse(subject),')','')) - 1))))) as Recruitment_Requisition_Num_Text,
Cast(Trim(STG.CreatedDateTime) as TimeStamp(0)) as Completed_Date,
NULL AS Candidate_SID,
NULL AS Resource_Screening_Package_Num,
NULL AS Sequence_Num,
'W' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
From EDWHR_STAGING.Enwisen_CM5Tickets_Stg STG
INNER JOIN EDWHR_STAGING.Ref_SK_XWLK xwlk
On Coalesce(Trim(Reverse(Trim(Substring(OREPLACE(Reverse(subject),')','') FROM 1 FOR ( Position('-' IN OREPLACE(Reverse(subject),')','')) - 1)))))||
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5)))))||'B',-1)  = xwlk.sk_source_txt and xwlk.sk_type = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN EDWHR_Base_Views.Ref_Onboarding_Event_Type ROE
On Event_Type_Code='B'
WHERE Topic  = 'Z-Auto-BG Check Auth'
AND Category  = 'Z-Auto-BG Check Auth'
AND SubCategory = 'Z-Auto-BG Check Auth'
AND Subject like 'BG Auth Complete%'      
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'
AND InStr(Subject,'-') <> 0
Qualify Row_Number() Over(Partition by xwlk.sk Order By STG.CreatedDateTime Desc)  = 1;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	


---HDM-1921

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWHR_STAGING','ATS_RESOURCETRANSITION_BCT_STG');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWHR_STAGING','ATS_RESOURCESCREENING_BCT_STG');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	

DELETE FROM EDWHR_STAGING.CANDIDATE_ONBOARDING_EVENT_XWLK_ATS;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	

INSERT INTO  EDWHR_STAGING.CANDIDATE_ONBOARDING_EVENT_XWLK_ATS
(
JOBREQUISITION,
CANDIDATE,
EVENT_TYPE_ID,
DW_LAST_UPDATE_DATE_TIME
)
SELECT 
TRIM(CAST (JOBREQUISITION AS VARCHAR(20))) AS JOBREQUISITION,
TRIM(CAST(CANDIDATE AS VARCHAR(20))) AS CANDIDATE,
CAST('2' AS CHAR(1))AS EVENT_TYPE_ID,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.ATS_RESOURCETRANSITION_BCT_STG 
WHERE STATUS_STATE = 'Complete'
GROUP BY 1,2,3,4
UNION
SELECT 
TRIM(CAST (RESOURCESCREENINGPACKAGE AS VARCHAR(20))) AS JOBREQUISITION,
TRIM(CAST(SEQUENCENUMBER AS VARCHAR(20))) AS CANDIDATE,
CAST('1' AS CHAR(1))AS EVENT_TYPE_ID,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.ATS_RESOURCESCREENING_BCT_STG 
WHERE HCASCREVENDSTAT= 'COMPLETED' AND SCREENING = 'DRUGSCREEN'
GROUP BY 1,2,3,4;
/*UNION
SELECT 
TRIM(CAST (RESOURCESCREENINGPACKAGE AS VARCHAR(20))) AS JOBREQUISITION,
TRIM(CAST(SEQUENCENUMBER AS VARCHAR(20))) AS CANDIDATE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.ATS_RESOURCESCREENING_BCT_STG 
WHERE HCASCREVENDSTAT = 'HRREVIEWRECRUITER'
GROUP BY 1,2,3;*/


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWHR_STAGING','CANDIDATE_ONBOARDING_EVENT_XWLK_ATS');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','CANDIDATE_ONBOARDING_EVENT_XWLK_ATS','TRIM(JOBREQUISITION)||TRIM(CANDIDATE)||EVENT_TYPE_ID||''-B''', 'CANDIDATE_ONBOARDING_EVENT');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

DELETE FROM EDWHR_STAGING.CANDIDATE_ONBOARDING_EVENT_XWLK_STG;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

INSERT INTO  EDWHR_STAGING.CANDIDATE_ONBOARDING_EVENT_XWLK_STG
(
REQUISITION_NUM,
CANDIDATE_NUM,
EVENT_TYPE_ID,
RECRUITMENT_REQUISITION_NUM_TEXT,
CREATION_DATE_TIME,
CANDIDATE_SID,
DW_LAST_UPDATE_DATE_TIME
)
SEL TRIM(CAST(S.REQUISITION_NUM AS VARCHAR(20))) AS REQUISITION_NUM , 
TRIM(CAST(S.CANDIDATE_NUM AS VARCHAR(20))) AS CANDIDATE_NUM,
CAST('3' AS CHAR(1))AS EVENT_TYPE_ID,
RR.RECRUITMENT_REQUISITION_NUM_TEXT AS RECRUITMENT_REQUISITION_NUM_TEXT,
ST.CREATION_DATE_TIME AS CREATION_DATE_TIME,
S.CANDIDATE_SID AS CANDIDATE_SID,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_BASE_VIEWS.SUBMISSION_TRACKING ST

INNER JOIN EDWHR_BASE_VIEWS.SUBMISSION_TRACKING_STATUS STS
ON ST.SUBMISSION_TRACKING_SID = STS.SUBMISSION_TRACKING_SID
AND STS.VALID_TO_DATE = '9999-12-31'
AND STS.SOURCE_SYSTEM_CODE='B'

INNER JOIN EDWHR_BASE_VIEWS.REF_SUBMISSION_STATUS RSS
ON STS.SUBMISSION_STATUS_ID = RSS.SUBMISSION_STATUS_ID
AND RSS.SOURCE_SYSTEM_CODE='B'

INNER JOIN EDWHR_BASE_VIEWS.CANDIDATE_PROFILE CP
ON ST.CANDIDATE_PROFILE_SID = CP.CANDIDATE_PROFILE_SID
AND CP.VALID_TO_DATE = '9999-12-31'
AND CP.SOURCE_SYSTEM_CODE='B'

INNER JOIN EDWHR_BASE_VIEWS.SUBMISSION S
ON CP.CANDIDATE_PROFILE_SID = S.CANDIDATE_PROFILE_SID
AND S.VALID_TO_DATE = '9999-12-31'
AND S.SOURCE_SYSTEM_CODE='B'

LEFT JOIN EDWHR_BASE_VIEWS.RECRUITMENT_REQUISITION RR
ON S.RECRUITMENT_REQUISITION_SID = RR.RECRUITMENT_REQUISITION_SID
AND RR.VALID_TO_DATE = '9999-12-31'
AND RR.SOURCE_SYSTEM_CODE='B'

WHERE ST.VALID_TO_DATE = '9999-12-31' AND ST.SOURCE_SYSTEM_CODE='B'
AND RSS.SUBMISSION_STATUS_DESC = 'candidate consent pending'
GROUP BY 1,2,3,4,5,6,7;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWHR_STAGING','CANDIDATE_ONBOARDING_EVENT_XWLK_STG');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','CANDIDATE_ONBOARDING_EVENT_XWLK_STG','REQUISITION_NUM||CANDIDATE_NUM||EVENT_TYPE_ID||''-B''', 'CANDIDATE_ONBOARDING_EVENT');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


INSERT INTO EDWHR_STAGING.CANDIDATE_ONBOARDING_EVENT_WRK
(
CANDIDATE_ONBOARDING_EVENT_SID,
VALID_FROM_DATE,
EVENT_TYPE_ID,
RECRUITMENT_REQUISITION_NUM_TEXT,
COMPLETED_DATE,
CANDIDATE_SID,
RESOURCE_SCREENING_PACKAGE_NUM,
SEQUENCE_NUM,
SOURCE_SYSTEM_CODE,
DW_LAST_UPDATE_DATE_TIME
)

SELECT
XWLK.SK AS CANDIDATE_ONBOARDING_EVENT_SID,
CURRENT_DATE AS VALID_FROM_DATE,
CAST('2' AS CHAR(1))AS EVENT_TYPE_ID,
REQ.RECRUITMENT_REQUISITION_NUM_TEXT AS RECRUITMENT_REQUISITION_NUM_TEXT,
CAST(STG.COMPLETION_DATE AS TIMESTAMP(0))AS COMPLETED_DATE,
CAN.CANDIDATE_SID AS CANDIDATE_SID,
ORC.RESOURCE_SCREENING_PACKAGE_NUM AS RESOURCE_SCREENING_PACKAGE_NUM,
NULL AS SEQUENCE_NUM,
'B' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.ATS_RESOURCETRANSITION_BCT_STG STG
INNER JOIN EDWHR_STAGING.REF_SK_XWLK XWLK
ON  TRIM(STG.JOBREQUISITION)||TRIM(CAST(STG.CANDIDATE AS VARCHAR(20)))||'2'||'-B'  = XWLK.SK_SOURCE_TXT   
AND XWLK.SK_TYPE = 'CANDIDATE_ONBOARDING_EVENT'
LEFT JOIN EDWHR_BASE_VIEWS.RECRUITMENT_REQUISITION REQ
ON REQ.REQUISITION_NUM=STG.JOBREQUISITION
AND  REQ.VALID_TO_DATE = '9999-12-31' 
AND REQ.SOURCE_SYSTEM_CODE = 'B'
LEFT JOIN EDWHR_BASE_VIEWS.CANDIDATE CAN
ON CAN.CANDIDATE_NUM=STG.CANDIDATE
AND  CAN.VALID_TO_DATE = '9999-12-31' 
AND CAN.SOURCE_SYSTEM_CODE = 'B'
LEFT JOIN EDWHR_BASE_VIEWS.CANDIDATE_ONBOARDING_RESOURCE ORC
ON ORC.CANDIDATE_SID=CAN.CANDIDATE_SID
AND  ORC.VALID_TO_DATE = '9999-12-31' 
AND ORC.SOURCE_SYSTEM_CODE = 'B'
WHERE STG.STATUS_STATE = 'Complete'
QUALIFY ROW_NUMBER() OVER(PARTITION BY STG.JOBREQUISITION, STG.CANDIDATE  ORDER BY STG.UPDATESTAMP DESC) = 1

UNION

SELECT
XWLK.SK AS CANDIDATE_ONBOARDING_EVENT_SID,
CURRENT_DATE AS VALID_FROM_DATE,
CAST('1' AS CHAR(1))AS EVENT_TYPE_ID,
REQ.RECRUITMENT_REQUISITION_NUM_TEXT AS RECRUITMENT_REQUISITION_NUM_TEXT,
CAST(STG.HCASCREVENDSTATDATE AS TIMESTAMP(0))AS COMPLETED_DATE,
ORC.CANDIDATE_SID AS CANDIDATE_SID,
STG.RESOURCESCREENINGPACKAGE AS RESOURCE_SCREENING_PACKAGE_NUM,
STG.SEQUENCENUMBER  AS SEQUENCE_NUM,
'B' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.ATS_RESOURCESCREENING_BCT_STG STG
INNER JOIN EDWHR_STAGING.REF_SK_XWLK XWLK
ON  TRIM(CAST(STG.RESOURCESCREENINGPACKAGE AS VARCHAR(20)))||TRIM(CAST(STG.SEQUENCENUMBER AS VARCHAR(20)))||'1'||'-B'  = XWLK.SK_SOURCE_TXT   
AND XWLK.SK_TYPE = 'CANDIDATE_ONBOARDING_EVENT'
LEFT JOIN EDWHR_BASE_VIEWS.CANDIDATE_ONBOARDING_RESOURCE ORC
ON ORC.RESOURCE_SCREENING_PACKAGE_NUM=STG.RESOURCESCREENINGPACKAGE
AND  ORC.VALID_TO_DATE = '9999-12-31' 
AND ORC.SOURCE_SYSTEM_CODE = 'B'
LEFT JOIN EDWHR_BASE_VIEWS.RECRUITMENT_REQUISITION REQ
ON REQ.RECRUITMENT_REQUISITION_SID=ORC.RECRUITMENT_REQUISITION_SID
AND  REQ.VALID_TO_DATE = '9999-12-31' 
AND REQ.SOURCE_SYSTEM_CODE = 'B'
WHERE STG.HCASCREVENDSTAT= 'COMPLETED' AND STG.SCREENING = 'DRUGSCREEN'
QUALIFY ROW_NUMBER() OVER(PARTITION BY STG.RESOURCESCREENINGPACKAGE, STG.SEQUENCENUMBER  ORDER BY STG.UPDATESTAMP DESC) = 1

UNION
SELECT
XWLK.SK AS CANDIDATE_ONBOARDING_EVENT_SID,
CURRENT_DATE AS VALID_FROM_DATE,
STG.EVENT_TYPE_ID AS EVENT_TYPE_ID,
STG.RECRUITMENT_REQUISITION_NUM_TEXT AS RECRUITMENT_REQUISITION_NUM_TEXT,
STG.CREATION_DATE_TIME AS COMPLETED_DATE,
STG.CANDIDATE_SID AS CANDIDATE_SID,
NULL AS RESOURCE_SCREENING_PACKAGE_NUM,
NULL AS SEQUENCE_NUM,
'B' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.CANDIDATE_ONBOARDING_EVENT_XWLK_STG STG
INNER JOIN EDWHR_STAGING.REF_SK_XWLK XWLK
ON   STG.REQUISITION_NUM||STG.CANDIDATE_NUM||STG.EVENT_TYPE_ID||'-B'  = XWLK.SK_SOURCE_TXT   
AND XWLK.SK_TYPE = 'CANDIDATE_ONBOARDING_EVENT'
QUALIFY ROW_NUMBER() OVER(PARTITION BY STG.REQUISITION_NUM, STG.CANDIDATE_NUM  ORDER BY STG.CREATION_DATE_TIME DESC) = 1;
--GROUP BY 1,2,3,4,5,6,7,8,9,10;

/*SELECT
XWLK.SK AS CANDIDATE_ONBOARDING_EVENT_SID,
CURRENT_DATE AS VALID_FROM_DATE,
CAST('3' AS CHAR(1))AS EVENT_TYPE_ID,
REQ.RECRUITMENT_REQUISITION_NUM_TEXT AS RECRUITMENT_REQUISITION_NUM_TEXT,
CAST(STG.HCASCREVENDSTATDATE AS TIMESTAMP(0))AS COMPLETED_DATE,
ORC.CANDIDATE_SID AS CANDIDATE_SID,
STG.RESOURCESCREENINGPACKAGE AS RESOURCE_SCREENING_PACKAGE_NUM,
STG.SEQUENCENUMBER  AS SEQUENCE_NUM,
'B' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.ATS_RESOURCESCREENING_BCT_STG STG
INNER JOIN EDWHR_STAGING.REF_SK_XWLK XWLK
ON  TRIM(CAST(STG.RESOURCESCREENINGPACKAGE AS VARCHAR(20))) ||TRIM(CAST(STG.SEQUENCENUMBER AS VARCHAR(20)))||'-B'  = XWLK.SK_SOURCE_TXT   
AND XWLK.SK_TYPE = 'CANDIDATE_ONBOARDING_EVENT'
LEFT JOIN EDWHR_BASE_VIEWS.CANDIDATE_ONBOARDING_RESOURCE ORC
ON ORC.RESOURCE_SCREENING_PACKAGE_NUM=STG.RESOURCESCREENINGPACKAGE
AND  ORC.VALID_TO_DATE = '9999-12-31' 
AND ORC.SOURCE_SYSTEM_CODE = 'B'
LEFT JOIN EDWHR_BASE_VIEWS.RECRUITMENT_REQUISITION REQ
ON REQ.RECRUITMENT_REQUISITION_SID=ORC.RECRUITMENT_REQUISITION_SID
AND  REQ.VALID_TO_DATE = '9999-12-31' 
AND REQ.SOURCE_SYSTEM_CODE = 'B'
WHERE STG.HCASCREVENDSTAT = 'HRREVIEWRECRUITER'
GROUP BY 1,2,3,4,5,6,7,8,9,10;*/


--Inserting Rejected Records which are filtered above

Insert Into EDWHR_STAGING.Enwisen_CM5Tickets_Stg_Reject
(
TicketCode,
UserID,
Archived,
CaseType,
TicketStatus,
CreatedDateTime,
SLADate,
ClosedDateTime,
LastEditDateTime,
ResolvedDateTime,
Topic,
Category,
SubCategory,
Source_Code,
ServiceGroup,
SubStatus,
IsFirstCallResolution,
CreatorUserID,
CloseUserID,
LastEditUserID,
OwnerUserID,
ChatAgentUserID,
RegardingUserID,
CreatorFirstName,
CreatorLastName,
CreatorName,
ContactMethod,
ContactName,
ContactRelationshipName,
AboutEE,
Email,
FirstName,
LastName,
SurveyDateTime,
SurveyID,
SurveyAnswer1,
SurveyAnswer2,
SurveyAnswer3,
SurveyAnswer4,
SurveyAnswer5,
SurveyScore,
SurveyAgreementResponse,
Population,
Priority,
ProcessTime,
ReminderDateTime,
ReminderEmail,
ReminderPhone,
Secure,
ShowToEE,
CustomCheckBox1,
CustomCheckBox2,
CustomCheckBox3,
CustomCheckBox4,
CustomCheckBox5,
CustomCheckBox6,
CustomDate1,
CustomDate2,
CustomDate3,
CustomDate4,
CustomDate5,
CustomDate6,
CustomSelect1,
CustomSelect2,
CustomSelect3,
CustomSelect4,
CustomSelect5,
CustomSelect6,
CustomString1,
CustomString2,
CustomString3,
CustomString4,
CustomString5,
CustomString6,
SurveyFollowup,
KnowledgeDomain,
ReminderNote,
SurveyCommentResponse,
Subject,
Resolution,
Issue,
DW_Last_Update_Date_Time)

SELECT distinct
TicketCode,
UserID,
Archived,
CaseType,
TicketStatus,
CreatedDateTime,
SLADate,
ClosedDateTime,
LastEditDateTime,
ResolvedDateTime,
Topic,
Category,
SubCategory,
Source_Code,
ServiceGroup,
SubStatus,
IsFirstCallResolution,
CreatorUserID,
CloseUserID,
LastEditUserID,
OwnerUserID,
ChatAgentUserID,
RegardingUserID,
CreatorFirstName,
CreatorLastName,
CreatorName,
ContactMethod,
ContactName,
ContactRelationshipName,
AboutEE,
Email,
FirstName,
LastName,
SurveyDateTime,
SurveyID,
SurveyAnswer1,
SurveyAnswer2,
SurveyAnswer3,
SurveyAnswer4,
SurveyAnswer5,
SurveyScore,
SurveyAgreementResponse,
Population,
Priority,
ProcessTime,
ReminderDateTime,
ReminderEmail,
ReminderPhone,
Secure,
ShowToEE,
CustomCheckBox1,
CustomCheckBox2,
CustomCheckBox3,
CustomCheckBox4,
CustomCheckBox5,
CustomCheckBox6,
CustomDate1,
CustomDate2,
CustomDate3,
CustomDate4,
CustomDate5,
CustomDate6,
CustomSelect1,
CustomSelect2,
CustomSelect3,
CustomSelect4,
CustomSelect5,
CustomSelect6,
CustomString1,
CustomString2,
CustomString3,
CustomString4,
CustomString5,
CustomString6,
SurveyFollowup,
KnowledgeDomain,
ReminderNote,
SurveyCommentResponse,
Subject,
Resolution,
Issue,
DW_Last_Update_Date_Time
from
(
sel distinct
Trim(Reverse(Trim(Substring(Reverse(subject) FROM 1 FOR ( Position('-' IN Reverse(subject)) - 1))))) as Requisition_Number,
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5))))) as Process_Level_Code,
STG.*
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg STG
WHERE Topic = 'Z-Auto-Onboarding Confirm'
AND Category = 'Z-Auto-Onboarding Confirm'
AND SubCategory = 'Z-Auto-Onboarding Confirm'
AND InStr(Subject,'-') <> 0
AND Subject like  'Onboarding Action Required %'     
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'

Union

sel distinct
Trim(Reverse(Trim(Substring(Reverse(subject) FROM 1 FOR ( Position('-' IN Reverse(subject)) - 1))))) as Requisition_Number,
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5))))) as Process_Level_Code,
STG.*
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg STG
WHERE Topic = 'Hire My Employees'
AND Category = 'Hiring Employees'
AND SubCategory = 'Drug Screen Results'
AND Subject Like  'CONFIRMED DS Results%'      
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'

Union

sel distinct
Trim(Reverse(Trim(Substring(OREPLACE(Reverse(subject),')','') FROM 1 FOR ( Position('-' IN OREPLACE(Reverse(subject),')','')) - 1))))) as Requisition_Number,
Trim(Reverse(Trim(Substring(Reverse(subject) FROM ( Position('-' IN Reverse(subject)) + 1) FOR (5))))) as Process_Level_Code,
STG.*
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg STG
WHERE Topic  = 'Z-Auto-BG Check Auth'
AND Category  = 'Z-Auto-BG Check Auth'
AND SubCategory = 'Z-Auto-BG Check Auth'
AND Subject like 'BG Auth Complete%'      
AND Subject NOT like '%ACQ -%'
AND Subject NOT like '%ACQ-%'
AND Subject NOT like '%ACQ %'
AND Subject NOT like '%Acquisition%'
AND InStr(Subject,'-') <> 0
) iq
where char_length(Requisition_Number) =0
OR char_length(Process_Level_Code) = 0
OR LOWER(Requisition_Number) <> Upper(Requisition_Number) (CASESPECIFIC)
OR LOWER(Process_Level_Code) <> Upper(Process_Level_Code) (CASESPECIFIC)
OR Requisition_Number  like '%*%'
OR Requisition_Number  like '%/%'
OR Requisition_Number  like '%)%'
OR Requisition_Number  like '%(%'
OR Requisition_Number  like '%.%'
OR Requisition_Number  like '%]%'
OR Requisition_Number  like '%[%'
OR Requisition_Number  like '% %'
OR Requisition_Number  like '%:%'
OR Requisition_Number  like '%''%'
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWHR_STAGING','Candidate_Onboarding_Event_Wrk');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('EDWHR_STAGING','Enwisen_CM5Tickets_Stg_Reject');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	

.LOGOFF;

.EXIT

EOF

