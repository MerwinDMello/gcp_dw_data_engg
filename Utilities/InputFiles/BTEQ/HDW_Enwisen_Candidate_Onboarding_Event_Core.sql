############################################################################################
#                                                                                   	   #
#   Script Name - HDW_Enwisen_Candidate_Onboarding_Event_Core.sql                                       #
#   Job Name    - J_HDW_Enwisen_Candidate_Onboarding_Event                                              #
#   Target Table- $NCR_TGT_SCHEMA.Candidate_Onboarding_Event                                	   #
#   Developer   - SYNTEL                                                          	   #
#   JIRA	- HDM-1834								   #
#	Initial Version										    #
############################################################################################

bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

SET QUERY_BAND = 'App=HR_Enwisen_ETL; Job=J_HDW_Enwisen_Candidate_Onboarding_Event;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/* Retire the records those are changed */

/* Begin Transaction Block Starts Here */

BT;


UPDATE TGT
FROM
 $NCR_STG_SCHEMA.Candidate_Onboarding_Event_WRK WRK, 
 $NCR_TGT_SCHEMA.Candidate_Onboarding_Event TGT
 SET Valid_To_Date = Current_date - INTERVAL '1' DAY
,DW_Last_Update_Date_Time = Current_Timestamp(0)
WHERE
WRK.Candidate_Onboarding_Event_SID = TGT.Candidate_Onboarding_Event_SID
AND (
   COALESCE(TRIM(WRK.Event_Type_ID),'XXX') NOT= COALESCE(TRIM(TGT.Event_Type_ID),'XXX')
OR COALESCE(TRIM(WRK.Recruitment_Requisition_Num_Text),'XXX') <> COALESCE(TRIM(TGT.Recruitment_Requisition_Num_Text),'XXX')
OR COALESCE(Cast(WRK.Completed_Date as Varchar(19)),'9999-12-30') <> COALESCE(Cast(TGT.Completed_Date as Varchar(19)),'9999-12-30')
OR COALESCE(TRIM(WRK.CANDIDATE_SID),'XXX') NOT= COALESCE(TRIM(TGT.CANDIDATE_SID),'XXX')
OR COALESCE(TRIM(WRK.RESOURCE_SCREENING_PACKAGE_NUM),'XXX') NOT= COALESCE(TRIM(TGT.RESOURCE_SCREENING_PACKAGE_NUM),'XXX')
OR COALESCE(TRIM(WRK.SEQUENCE_NUM),'XXX') NOT= COALESCE(TRIM(TGT.SEQUENCE_NUM),'XXX')
OR COALESCE(TRIM(WRK.SOURCE_SYSTEM_CODE),'XXX') NOT= COALESCE(TRIM(TGT.SOURCE_SYSTEM_CODE),'XXX')
)
AND TGT.Valid_To_Date='9999-12-31';

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

Insert into $NCR_TGT_SCHEMA.Candidate_Onboarding_Event
(
Candidate_Onboarding_Event_SID,
Valid_From_Date,
Event_Type_ID,
Recruitment_Requisition_Num_Text,
Valid_To_Date,
Completed_Date,
CANDIDATE_SID,
RESOURCE_SCREENING_PACKAGE_NUM,
SEQUENCE_NUM,
Source_System_Code,
DW_Last_Update_Date_Time
)

Select
WRK.Candidate_Onboarding_Event_SID,
WRK.Valid_From_Date as Valid_From_Date,
WRK.Event_Type_ID,
WRK.Recruitment_Requisition_Num_Text,
'9999-12-31' as Valid_To_Date,
WRK.Completed_Date,
WRK.CANDIDATE_SID,
WRK.RESOURCE_SCREENING_PACKAGE_NUM,
WRK.SEQUENCE_NUM,
WRK.Source_System_Code,
Current_Timestamp(0) as DW_Last_Update_Date_Time
qualify Row_Number()over(partition by Candidate_Onboarding_Event_SID ,
Valid_From_Date  order by  Candidate_Onboarding_Event_SID ,
Valid_From_Date   desc)=1 

from 
$NCR_STG_SCHEMA.Candidate_Onboarding_Event_WRK WRK

where (
COALESCE(TRIM(WRK.Candidate_Onboarding_Event_SID),123456),
COALESCE(TRIM(WRK.Event_Type_ID),'XXX'),
COALESCE(TRIM(WRK.Recruitment_Requisition_Num_Text),'XXX'),
COALESCE(Cast(WRK.Completed_Date as Varchar(19)),'9999-12-30'),
COALESCE(TRIM(WRK.CANDIDATE_SID),123456),
COALESCE(TRIM(WRK.RESOURCE_SCREENING_PACKAGE_NUM),123456),
COALESCE(TRIM(WRK.SEQUENCE_NUM),123456),
COALESCE(TRIM(WRK.Source_System_Code),'XXX')
)
NOT IN
(Select
COALESCE(TRIM(TGT.Candidate_Onboarding_Event_SID),123456),
COALESCE(TRIM(TGT.Event_Type_ID),'XXX'),
COALESCE(TRIM(TGT.Recruitment_Requisition_Num_Text),'XXX'),
COALESCE(Cast(TGT.Completed_Date as Varchar(19)),'9999-12-30'),
COALESCE(TRIM(TGT.CANDIDATE_SID),123456),
COALESCE(TRIM(TGT.RESOURCE_SCREENING_PACKAGE_NUM),123456),
COALESCE(TRIM(TGT.SEQUENCE_NUM),123456),
COALESCE(TRIM(TGT.Source_System_Code),'XXX')
from EDWHR_BASE_VIEWS.Candidate_Onboarding_Event TGT where  TGT.Valid_To_Date='9999-12-31'
);

.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;

ET;

/* End Transaction Block comment */

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;


CALL DBADMIN_PROCS.COLLECT_STATS_TABLE('$NCR_TGT_SCHEMA','Candidate_Onboarding_Event');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;	


.LOGOFF;

.EXIT

EOF