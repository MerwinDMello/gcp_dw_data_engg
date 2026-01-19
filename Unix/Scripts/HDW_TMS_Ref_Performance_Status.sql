#####################################################################################
#                                               					#
#   SCRIPT NAME     - HDW_TMS_Ref_Performance_Rating.SQL                  	
#   Job NAME    	- J_HDW_TMS_Ref_Performance_Status                       	#
#   TARGET TABLE    - $NCR_TGT_SCHEMA.Ref_Performance_Status                    	#
#   Developer   	- Julia Kim                                 			#
#   Version 		- 1.0 - Initial RELEASE                     			#
#   Description 	- The SCRIPT loads the TARGET TABLE WITH NEW records		#
#####################################################################################


bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Performance_Status;' FOR SESSION;

/* Load Work Table with working Data */




DELETE FROM EDWHR_Staging.Ref_Performance_Status_WRK;

INSERT INTO EDWHR_Staging.Ref_Performance_Status_WRK 
(
Performance_Status_Id,
Performance_Status_Desc,
Source_System_Code,
DW_Last_Update_Date_Time
)
SELECT 
(SELECT (COALESCE(MAX(Performance_Status_Id),0)) FROM EDWHR_BASE_VIEWS.Ref_Performance_Status) + 
ROW_NUMBER() OVER (ORDER BY Y.Performance_Status_Desc) AS Performance_Status_Id
, Y.Performance_Status_Desc
, 'M' AS SOURCE_SYSTEM_CODE
, CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM (
SEL STG.Performance_Status_Desc
FROM 
(   
SELECT 
DISTINCT(TRIM(Status)) AS Performance_Status_Desc
FROM EDWHR_STAGING.Activities_Report
WHERE TRIM(Status) IS NOT NULL AND TRIM(Status) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Evaluation_Workflow_State)) AS Performance_Status_Desc
FROM EDWHR_STAGING.Competency_Ratings_Report
WHERE TRIM(Evaluation_Workflow_State) IS NOT NULL AND TRIM(Evaluation_Workflow_State) NOT = ''

UNION 


SELECT 
DISTINCT(TRIM(Goal_Status)) AS Performance_Status_Desc
FROM EDWHR_STAGING.Employee_Perf_Goals
WHERE TRIM(Goal_Status) IS NOT NULL AND TRIM(Goal_Status) NOT = ''
 

UNION

SELECT 
DISTINCT(TRIM(Goal_Progress)) AS Performance_Status_Desc
FROM EDWHR_STAGING.Employee_Perf_Goals
WHERE TRIM(Goal_Progress) IS NOT NULL AND TRIM(Goal_Progress) NOT = ''


UNION

SELECT 
DISTINCT(TRIM(Eval_Workflow_State)) AS Performance_Status_Desc
FROM EDWHR_STAGING.Performance_Ratings_Report
WHERE TRIM(Eval_Workflow_State) IS NOT NULL AND TRIM(Eval_Workflow_State) NOT = ''
) STG

LEFT OUTER JOIN EDWHR_Staging.Ref_Performance_Status_WRK   TGT
ON SUBSTR(STG.Performance_Status_Desc,1,100) = TGT.Performance_Status_Desc

WHERE TGT.Performance_Status_Desc IS NULL
) Y
;


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
/*  Collect Statistics on the WRK Table    */


CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_Performance_Status_Wrk');

Insert into EDWHR.Ref_Performance_Status
 ( 
Performance_Status_Id,
Performance_Status_Desc,
Source_System_Code,
DW_Last_Update_Date_Time

 )
select Row_Number() Over ( ORDER BY WRK.Performance_Status_Id) + (SELECT Coalesce(Max(Performance_Status_Id),0) FROM EDWHR.Ref_Performance_Status) AS Performance_Status_Id,
Trim(Performance_Status_Desc),
WRK.SOURCE_SYSTEM_CODE ,
current_timestamp(0) as dw_last_update_date_time
FROM EDWHR_Staging.Ref_Performance_Status_WRK WRK
where WRK.Performance_Status_Desc not in (sel Performance_Status_Desc from EDWHR.Ref_Performance_Status); 

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL dbadmin_procs.collect_stats_table ('$NCR_TGT_SCHEMA','Ref_Performance_Status');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF



