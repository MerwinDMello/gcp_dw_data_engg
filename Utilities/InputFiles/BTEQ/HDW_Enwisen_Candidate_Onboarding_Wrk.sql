############################################################################################
#                                                                                   	   #
#   Script Name - HDW_Enwisen_Candidate_Onboarding_Wrk.sql                                        #
#   Job Name    - J_HDW_Enwisen_Candidate_Onboarding                                              #
#   Target Table- $NCR_STG_SCHEMA.Candidate_Onboarding_WRK                               	   #
#   Developer   - Syntel                                                          	   #
#   JIRA	- HDM-1833								   #
#	Initial Version		   #
#   		   					   # 
############################################################################################

bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


SET QUERY_BAND = 'App=HR_Enwisen_ETL; Job=J_HDW_Enwisen_Candidate_Onboarding;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/*  Generate the surrogate keys for Candidate_Onboarding */

CALL EDWHR_PROCS.SK_GEN('$NCR_STG_SCHEMA','Enwisen_Audit','Coalesce(Trim(EmployeeID)||Trim(HRCONumber)||Trim(RequisitionNumber)||Trim(ApplicantNumber),-1)', 'CANDIDATE_ONBOARDING');
CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','ATS_ResourceTransition_BCT_STG','Coalesce(Trim(Cast(Candidate AS Varchar(15)))||Trim(JobRequisition),-1)||''-ATS''', 'CANDIDATE_ONBOARDING');

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/*  Truncate Work Table     */

Delete from $NCR_STG_SCHEMA.Candidate_Onboarding_Wrk;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/*  Load Work Table with working Data for Enwisen */

Create multiset volatile table CAN
AS
(SELECT DISTINCT
CP.CANDIDATE_SID,
TRIM(REQ.Requisition_Num) Requisition_Num,
TRIM(REQ.Lawson_Company_Num) Lawson_Company_Num,
TRIM(SUBSTR('000000000',1,9 - CHAR_LENGTH(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(Trim(CP.SOCIAL_SECURITY_NUM),'-',''),'.',''),'/',''),' ',''),'_',''))) 
|| TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(Trim(CP.SOCIAL_SECURITY_NUM),'-',''),'.',''),'/',''),' ',''),'_','')) AS SOCIAL_SECURITY_NUM
FROM EDWHR_Base_views.Candidate_Person CP  
INNER JOIN EDWHR_base_views.submission S ON 
Trim(S.candidate_sid) =Trim(CP.candidate_sid)
AND CP.Valid_To_Date = '9999-12-31'
AND CHAR_LENGTH(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(Trim(CP.SOCIAL_SECURITY_NUM),'-',''),'.',''),'/',''),' ',''),'_','')) < 10
AND S.Valid_To_Date = '9999-12-31'
AND CP.Source_System_Code = 'T'
AND S.Source_System_Code = 'T'
INNER JOIN EDWHR_base_views.Recruitment_Requisition RR ON 
Trim(S.Recruitment_Requisition_sid)= Trim(RR.Recruitment_Requisition_sid)
AND RR.Valid_To_Date = '9999-12-31'
AND RR.Source_System_Code = 'T'
INNER JOIN EDWHR_Base_views.Requisition req ON
Trim(REQ.Requisition_SID)=Trim(RR.lawson_requisition_sid)
AND Req.Valid_To_Date = '9999-12-31'
--AND Req.Source_System_Code = 'T'
)
with data and stats
primary index(SOCIAL_SECURITY_NUM, Requisition_Num, Lawson_Company_Num)
on commit preserve rows;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

COLLECT STATISTICS COLUMN (REQUISITION_NUM ,LAWSON_COMPANY_NUM,SOCIAL_SECURITY_NUM) ON CAN;


Insert into EDWHR_STAGING.Candidate_Onboarding_Wrk
(
Candidate_Onboarding_SID,
Valid_From_Date,
Requisition_SID,
Employee_SID,
Candidate_SID,
Candidate_First_Name,
Candidate_Last_Name,
Tour_Start_Date,
Tour_ID,
Tour_Status_ID,
Tour_Completion_Pct,
Workflow_ID,
Workflow_Status_ID,
Email_Sent_Status_ID,
Onboarding_Confirmation_Date,
Recruitment_Requisition_Num_Text,
Process_Level_Code,
Applicant_Num,
Source_System_Code,
DW_Last_Update_Date_Time
)
Select
xwlk.SK as Candidate_Onboarding_SID,
Current_Date as Valid_From_Date,
REQ.Requisition_SID as Requisition_SID,
EMP.Employee_SID as Employee_SID,
CAN.Candidate_SID as Candidate_SID,
Trim(STG.FirstName) as Candidate_First_Name,
Trim(STG.LastName) as Candidate_Last_Name,
Cast(STG.BaseDate as Date Format 'MM-DD-YY') + INTERVAL '100' YEAR as Tour_Start_Date,
Trim(ROT.Tour_ID) as Tour_ID,
Trim(ROTS.Tour_Status_ID) as Tour_Status_ID,
Cast(Trim(STG.TourPercent) as Decimal(18,3)) as Tour_Completion_Pct,
Trim(ROW1.Workflow_ID) as Workflow_ID,
Trim(ROWS1.Workflow_Status_ID) as Workflow_Status_ID,
Trim(REH.Email_Sent_Status_ID) as Email_Sent_Status_ID,
Cast(SUBSTR(STG.ApprovalDate,1,10) as Date Format 'YYYY-MM-DD') as Onboarding_Confirmation_Date,
Trim(STG.ProcessLevel)||'-'||Trim(STG.RequisitionNumber) as Recruitment_Requisition_Num_Text,
Trim(STG.ProcessLevel) as Process_Level_Code,
Cast(Trim(STG.ApplicantNumber) as Integer) as Applicant_Num,
'W' as Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
From EDWHR_STAGING.Enwisen_Audit  STG
INNER JOIN EDWHR_STAGING.Ref_SK_XWLK xwlk
On Coalesce(Trim(EmployeeID)||Trim(HRCONumber)||Trim(RequisitionNumber)||Trim(ApplicantNumber),-1)  = xwlk.sk_source_txt and xwlk.sk_type = 'CANDIDATE_ONBOARDING'

INNER JOIN EDWHR_Base_views.Requisition REQ
On Trim(STG.RequisitionNumber)=Trim(REQ.Requisition_Num)
and Trim(STG.HRCONumber)=Trim(REQ.Lawson_Company_Num)
and REQ.valid_to_date = '9999-12-31'

INNER JOIN EDWHR_Base_views.Ref_Onboarding_Tour ROT
On Trim(STG.TourName)=Trim(ROT.Tour_Name)

INNER JOIN EDWHR_Base_views.Ref_Onboarding_Tour_Status ROTS
On Trim(STG.TourStatus)=Trim(ROTS.Tour_Status_Text)
and  ROTS.Source_System_Code = 'W'

INNER JOIN EDWHR_Base_views.Ref_Onboarding_Workflow ROW1
On Trim(STG.WorkflowName)=Trim(ROW1.Workflow_Name)

INNER JOIN EDWHR_Base_views.Ref_Onboarding_Workflow_Status ROWS1
On Trim(STG.WorkflowStatus)=Trim(ROWS1.Workflow_Status_Text)

INNER JOIN EDWHR_Base_views.Ref_Email_To_HR_Status REH
On Trim(STG.HRStatus)=Trim(REH.Email_Sent_Status_Text)

LEFT JOIN EDWHR_Base_views.Employee EMP
On Trim(STG.EmployeeID)=Trim(EMP.Employee_Num)
and Trim(STG.HRCONumber)=Trim(EMP.Lawson_Company_Num)
and EMP.valid_to_date = '9999-12-31'

LEFT JOIN CAN
ON Trim(STG.SSN) =CAN.SOCIAL_SECURITY_NUM
AND Trim(STG.RequisitionNumber)=CAN.Requisition_Num
AND  Trim(STG.HRCONumber)=CAN.Lawson_Company_Num

where STG.ApprovalDate like '20%'
and STG.DataStatus='Sent'
Qualify Row_Number() Over(Partition by xwlk.sk Order By STG.ApprovalDate Desc)  = 1;


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	

	
/*  Load Work Table with working Data for ATS */

Insert into EDWHR_STAGING.Candidate_Onboarding_Wrk
(
Candidate_Onboarding_SID,
Valid_From_Date,
Requisition_SID,
Employee_SID,
Candidate_SID,
Candidate_First_Name,
Candidate_Last_Name,
Tour_Start_Date,
Tour_ID,
Tour_Status_ID,
Tour_Completion_Pct,
Workflow_ID,
Workflow_Status_ID,
Email_Sent_Status_ID,
Onboarding_Confirmation_Date,
Recruitment_Requisition_Num_Text,
Process_Level_Code,
Applicant_Num,
Source_System_Code,
DW_Last_Update_Date_Time
)
Select
xwlk.SK as Candidate_Onboarding_SID,
Current_Date as Valid_From_Date,
REQ.Lawson_Requisition_SID as Requisition_SID,
NULL as Employee_SID,
Trim(CAN.Candidate_SID) as Candidate_SID,
Trim(CANP.First_Name) as Candidate_First_Name,
Trim(CANP.Last_Name) as Candidate_Last_Name,
Cast(STG.CreateStamp as Date) as Tour_Start_Date,
NULL as Tour_ID,
Trim(ROTS.Tour_Status_ID) as Tour_Status_ID,
NULL as Tour_Completion_Pct,
NULL as Workflow_ID,
NULL as Workflow_Status_ID,
NULL as Email_Sent_Status_ID,
--Case WHEN STG.Status_State in ('Complete','Completed') Then STG.Completion_Date ELSE NULL END as Onboarding_Confirmation_Date,
Sub.Onboarding_Confirmation_Date as Onboarding_Confirmation_Date,
Trim(REQ.Recruitment_Requisition_Num_Text) as Recruitment_Requisition_Num_Text,
Trim(REQ.Process_Level_Code) as Process_Level_Code,
NULL as Applicant_Num,
'B' as Source_System_Code, 
Current_Timestamp(0) as DW_Last_Update_Date_Time
From EDWHR_STAGING.ATS_ResourceTransition_BCT_STG  STG
INNER JOIN EDWHR_STAGING.Ref_SK_XWLK xwlk
On Coalesce(Trim(Cast(Candidate AS Varchar(15)))||Trim(JobRequisition),-1)||'-ATS'  = xwlk.sk_source_txt and xwlk.sk_type = 'CANDIDATE_ONBOARDING'

LEFT OUTER JOIN EDWHR_Base_views.Recruitment_Requisition REQ
On Cast(Trim(STG.JobRequisition) as Integer) = Trim(REQ.Requisition_Num)
and REQ.valid_to_date = '9999-12-31'
and REQ.Source_System_Code = 'B' 

LEFT OUTER JOIN EDWHR_Base_views.Candidate CAN
On Trim(STG.Candidate) = Trim(CAN.Candidate_Num)
and CAN.valid_to_date = '9999-12-31'
and CAN.Source_System_Code = 'B' 

INNER  JOIN EDWHR_Base_views.Candidate_Person CANP
On Trim(CAN.Candidate_SID) = Trim(CANP.Candidate_SID)
and CANP.valid_to_date = '9999-12-31'
and CANP.Source_System_Code = 'B' 

LEFT OUTER JOIN EDWHR_Base_views.Ref_Onboarding_Tour_Status ROTS
On Trim(STG.Status_State)= Trim(ROTS.Tour_Status_Text)
and  ROTS.Source_System_Code = 'B'

LEFT OUTER JOIN (
SELECT
s.Candidate_Profile_SID,
st.Creation_Date_Time AS Onboarding_Confirmation_Date,
s.Recruitment_Requisition_SID,
s.Candidate_Num

FROM EDWHR_BASE_VIEWS.Submission s

LEFT JOIN EDWHR_BASE_VIEWS.Submission_Tracking st
ON s.Candidate_Profile_SID = st.Candidate_Profile_SID
AND st.Valid_To_Date = '9999-12-31'
AND st.Source_System_Code = 'B'


LEFT JOIN EDWHR_BASE_VIEWS.Submission_Tracking_Status sts
ON st.Submission_Tracking_Sid = sts.Submission_Tracking_Sid
AND sts.Valid_To_Date = '9999-12-31'
AND sts.Source_System_Code = 'B'

LEFT JOIN EDWHR_BASE_VIEWS.Ref_Submission_Status rss
ON rss.Submission_Status_id = sts.Submission_Status_id

WHERE rss.Submission_Status_Name = 'CONFIRMED COMPLETED'
AND s.Valid_To_Date = '9999-12-31'
AND s.Source_System_Code = 'B'

 QUALIFY Row_Number()Over(PARTITION BY s.Candidate_Profile_SID ORDER BY st.Creation_Date_Time ASC) = 1)Sub
 ON sub.Recruitment_Requisition_SID=REQ.Recruitment_Requisition_SID
 AND sub.Candidate_Num = stg.Candidate 
 and REQ.valid_to_date = '9999-12-31'
and REQ.Source_System_Code = 'B' 

Qualify Row_Number() Over(Partition by xwlk.sk Order By STG.UpdateStamp Desc)  = 1
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	
	
CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('$NCR_STG_SCHEMA','Candidate_Onboarding_Wrk');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	

.LOGOFF;

.EXIT

EOF

