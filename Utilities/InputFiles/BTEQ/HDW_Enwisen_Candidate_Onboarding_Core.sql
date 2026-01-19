############################################################################################
#                                                                                   	   #
#   Script Name - HDW_Enwisen_Candidate_Onboarding_Core.sql                                       #
#   Job Name    - J_HDW_Enwisen_Candidate_Onboarding                                              #
#   Target Table- $NCR_TGT_SCHEMA.Candidate_Onboarding                                	   #
#   Developer   - SYNTEL                                                          	   #
#   JIRA	- HDM-1833								   #
#	Initial Version										    #
############################################################################################

bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

SET QUERY_BAND = 'App=HR_Enwisen_ETL; Job=J_HDW_Enwisen_Candidate_Onboarding;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/* Retire the records those are changed */

/* Begin Transaction Block Starts Here */

BT;


UPDATE TGT
FROM
 $NCR_STG_SCHEMA.Candidate_Onboarding_WRK WRK, 
 $NCR_TGT_SCHEMA.Candidate_Onboarding TGT
 SET Valid_To_Date = Current_date - INTERVAL '1' DAY
,DW_Last_Update_Date_Time = Current_Timestamp(0)
WHERE
WRK.Candidate_Onboarding_SID = TGT.Candidate_Onboarding_SID
AND (
   COALESCE(TRIM(WRK.Requisition_SID),123456) NOT=COALESCE(TRIM(TGT.Requisition_SID),123456)
OR COALESCE(TRIM(WRK.Employee_SID),123456) <>COALESCE(TRIM(TGT.Employee_SID),123456)
OR COALESCE(TRIM(WRK.Candidate_SID),123456) <>COALESCE(TRIM(TGT.Candidate_SID),123456)
OR COALESCE(TRIM(WRK.Candidate_First_Name),'XXX') NOT=COALESCE(TRIM(TGT.Candidate_First_Name),'XXX')
OR COALESCE(TRIM(WRK.Candidate_Last_Name),'XXX') <>COALESCE(TRIM(TGT.Candidate_Last_Name),'XXX')
OR COALESCE(TRIM(WRK.Tour_Start_Date),'9999-12-30') <>COALESCE(TRIM(TGT.Tour_Start_Date),'9999-12-30')
OR COALESCE(TRIM(WRK.Tour_ID),123456) <> COALESCE(TRIM(TGT.Tour_ID),123456)
OR COALESCE(TRIM(WRK.Tour_Status_ID),123456) <> COALESCE(TRIM(TGT.Tour_Status_ID),123456)
OR COALESCE(TRIM(WRK.Tour_Completion_Pct),123456) <> COALESCE(TRIM(TGT.Tour_Completion_Pct),123456)
OR COALESCE(TRIM(WRK.Workflow_ID),123456) <>COALESCE(TRIM(TGT.Workflow_ID),123456)
OR COALESCE(TRIM(WRK.Workflow_Status_ID),123456) <> COALESCE(TRIM(TGT.Workflow_Status_ID),123456)
OR COALESCE(TRIM(WRK.Email_Sent_Status_ID),123456) <> COALESCE(TRIM(TGT.Email_Sent_Status_ID),123456)
OR COALESCE(TRIM(WRK.Onboarding_Confirmation_Date),'9999-12-30') <>COALESCE(TRIM(TGT.Onboarding_Confirmation_Date),'9999-12-30')
OR COALESCE(TRIM(WRK.Recruitment_Requisition_Num_Text),'XXX') <>COALESCE(TRIM(TGT.Recruitment_Requisition_Num_Text),'XXX')
OR COALESCE(TRIM(WRK.Process_Level_Code),'XXX') <>COALESCE(TRIM(TGT.Process_Level_Code),'XXX')
OR COALESCE(TRIM(WRK.Applicant_Num),123456) <>COALESCE(TRIM(TGT.Applicant_Num),123456)
OR COALESCE(TRIM(WRK.SOURCE_SYSTEM_CODE),'XXX') NOT=COALESCE(TRIM(TGT.SOURCE_SYSTEM_CODE),'XXX')
)
AND TGT.Valid_To_Date='9999-12-31';

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

Insert into $NCR_TGT_SCHEMA.Candidate_Onboarding
(

Candidate_Onboarding_SID,
Valid_From_Date,
Valid_To_Date,
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
WRK.Candidate_Onboarding_SID,
WRK.Valid_From_Date as Valid_From_Date,
'9999-12-31' as Valid_To_Date,
WRK.Requisition_SID,
WRK.Employee_SID,
WRK.Candidate_SID,
WRK.Candidate_First_Name,
WRK.Candidate_Last_Name,
WRK.Tour_Start_Date,
WRK.Tour_ID,
WRK.Tour_Status_ID,
WRK.Tour_Completion_Pct,
WRK.Workflow_ID,
WRK.Workflow_Status_ID,
WRK.Email_Sent_Status_ID,
WRK.Onboarding_Confirmation_Date,
WRK.Recruitment_Requisition_Num_Text,
WRK.Process_Level_Code,
WRK.Applicant_Num,
WRK.Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time

from 
$NCR_STG_SCHEMA.Candidate_Onboarding_WRK WRK

where (
COALESCE(TRIM(WRK.Candidate_Onboarding_SID),123456),
COALESCE(TRIM(WRK.Requisition_SID),123456),
COALESCE(TRIM(WRK.Employee_SID),123456),
COALESCE(TRIM(WRK.Candidate_SID),123456),
COALESCE(TRIM(WRK.Candidate_First_Name),'XXX'),
COALESCE(TRIM(WRK.Candidate_Last_Name),'XXX'),
COALESCE(TRIM(WRK.Tour_Start_Date),'9999-12-30'),
COALESCE(TRIM(WRK.Tour_ID),123456),
COALESCE(TRIM(WRK.Tour_Status_ID),123456),
COALESCE(TRIM(WRK.Tour_Completion_Pct),123456),
COALESCE(TRIM(WRK.Workflow_ID),123456),
COALESCE(TRIM(WRK.Workflow_Status_ID),123456),
COALESCE(TRIM(WRK.Email_Sent_Status_ID),123456),
COALESCE(TRIM(WRK.Onboarding_Confirmation_Date),'9999-12-30'),
COALESCE(TRIM(WRK.Recruitment_Requisition_Num_Text),'XXX'),
COALESCE(TRIM(WRK.Process_Level_Code),'XXX'),
COALESCE(TRIM(WRK.Applicant_Num),123456),
COALESCE(TRIM(WRK.Source_System_Code),'XXX')
)
NOT IN
(Select
COALESCE(TRIM(TGT.Candidate_Onboarding_SID),123456),
COALESCE(TRIM(TGT.Requisition_SID),123456),
COALESCE(TRIM(TGT.Employee_SID),123456),
COALESCE(TRIM(TGT.Candidate_SID),123456),
COALESCE(TRIM(TGT.Candidate_First_Name),'XXX'),
COALESCE(TRIM(TGT.Candidate_Last_Name),'XXX'),
COALESCE(TRIM(TGT.Tour_Start_Date),'9999-12-30'),
COALESCE(TRIM(TGT.Tour_ID),123456),
COALESCE(TRIM(TGT.Tour_Status_ID),123456),
COALESCE(TRIM(TGT.Tour_Completion_Pct),123456),
COALESCE(TRIM(TGT.Workflow_ID),123456),
COALESCE(TRIM(TGT.Workflow_Status_ID),123456),
COALESCE(TRIM(TGT.Email_Sent_Status_ID),123456),
COALESCE(TRIM(TGT.Onboarding_Confirmation_Date),'9999-12-30'),
COALESCE(TRIM(TGT.Recruitment_Requisition_Num_Text),'XXX'),
COALESCE(TRIM(TGT.Process_Level_Code),'XXX'),
COALESCE(TRIM(TGT.Applicant_Num),123456),
COALESCE(TRIM(TGT.Source_System_Code),'XXX')
from EDWHR_BASE_VIEWS.Candidate_Onboarding TGT where  TGT.Valid_To_Date='9999-12-31'
);

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

ET;

/* End Transaction Block comment */

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;


CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('$NCR_TGT_SCHEMA','Candidate_Onboarding');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	


.LOGOFF;

.EXIT

EOF