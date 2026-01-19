#   SCRIPT NAME     - HDW_Enwisen_Ref_Onboarding_Event_Type.sql                  		#
#   Job NAME        - J_HDW_Enwisen_Ref_Onboarding_Event_Type                            	#
#   TARGET TABLE    - EDWHR.Ref_Onboarding_Event_Type                       			#
#   Developer   	- Syntel                                                              # 
#   Version 		- 1.0 - Initial RELEASE                     				#
#   Description 	- The SCRIPT loads the TARGET with Upsert					#
############################################################################################

bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

SET QUERY_BAND = 'App=HR_Enwisen_ETL; Job=J_HDW_Enwisen_Ref_Onboarding_Event_Type;' FOR SESSION;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


/*  Merge the data into EDWHR.Ref_Onboarding_Event_Type table  */

MERGE INTO EDWHR.Ref_Onboarding_Event_Type TGT
USING (
SELECT
CASE  WHEN TGT.Event_Type_ID IS  NOT NULL THEN TGT.Event_Type_ID ELSE
((SELECT  COALESCE(MAX(Event_Type_ID ),0)  FROM  EDWHR_BASE_VIEWS.Ref_Onboarding_Event_Type)
+ ROW_NUMBER() OVER ( ORDER BY Event_Type_ID  NULLS FIRST,STG.Event_Type_Code)) END AS Event_Type_ID,
STG.Event_Type_Code AS Event_Type_Code,
STG.Event_Type_Desc as Event_Type_Desc,
STG.Source_System_Code,
CURRENT_TIMESTAMP(0) AS DW_Last_Update_Date_Time FROM
(
sel distinct
'D' as Event_Type_Code,
cast('Drug Screen Completion' as VARCHAR(30)) as Event_Type_Desc,
'W' as Source_System_Code
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg
Union
sel distinct
'T' as Event_Type_Code,
cast('Tour Completion' as VARCHAR(30)) as Event_Type_Desc,
'W' as Source_System_Code
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg
Union
sel distinct
'B' as Event_Type_Code,
cast('Authorized Background Check' as VARCHAR(30)) as Event_Type_Desc,
'W' as Source_System_Code
from EDWHR_STAGING.Enwisen_CM5Tickets_Stg
)STG
LEFT JOIN EDWHR_BASE_VIEWS.Ref_Onboarding_Event_Type TGT
ON TGT.Event_Type_Code = STG.Event_Type_Code
) SRC
ON TGT.Event_Type_ID = SRC.Event_Type_ID

WHEN MATCHED THEN 
UPDATE SET 
Event_Type_Desc=SRC.Event_Type_Desc,
DW_Last_Update_Date_Time=SRC.DW_Last_Update_Date_Time

WHEN NOT MATCHED THEN
INSERT
(
TGT.Event_Type_ID,
TGT.Event_Type_Code,
TGT.Event_Type_Desc,
TGT.Source_System_Code,
TGT.DW_Last_Update_Date_Time
)
VALUES
(
SRC.Event_Type_ID,
SRC.Event_Type_Code,
SRC.Event_Type_Desc,
SRC.Source_System_Code,
SRC.DW_Last_Update_Date_Time
);

.IF ERRORCODE <> 0 Then .Quit ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Ref_Onboarding_Event_Type');
 
.IF ERRORCODE <> 0 THEN .Quit ERRORCODE;


.Logoff;

.exit

EOF

