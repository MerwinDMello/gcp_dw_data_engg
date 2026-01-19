
#####################################################################################
#                                               					#
#   SCRIPT NAME     - HDW_TMS_Ref_Competency_Group.SQL                  	
#   Job NAME    	- J_HDW_TMS_Ref_Competency_Group                       	#
#   TARGET TABLE    - $NCR_TGT_SCHEMA.Ref_Competency_Group                    	#
#   Developer   	- Julia Kim                                 			#
#   Version 		- 1.0 - Initial RELEASE                     			#
#   Description 	- The SCRIPT loads the TARGET TABLE WITH NEW records		#
#####################################################################################


bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Competency_Group;' FOR SESSION;

/* Load Work Table with working Data */




DELETE FROM EDWHR_Staging.Ref_Competency_Group_WRK;

INSERT INTO EDWHR_STAGING.Ref_Competency_Group_Wrk
(
Competency_Group_Id,
Competency_Group_Desc,
Source_System_Code,
DW_LAST_UPDATE_DATE_TIME
)
SELECT 
(SELECT (COALESCE(MAX(Competency_Group_Id),0)) FROM EDWHR_BASE_VIEWS.Ref_Competency_Group) + 
ROW_NUMBER() OVER (ORDER BY Y.Competency_Group_Desc) AS Competency_Group_Id
,Y.Competency_Group_Desc
,'M' AS SOURCE_SYSTEM_CODE
,current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME

FROM (
SEL STG.Competency_Group_Desc
FROM
(
SELECT 
DISTINCT(TRIM(Competency_Group)) AS Competency_Group_Desc
FROM EDWHR_STAGING.Competency_Ratings_Report
WHERE TRIM(Competency_Group) IS NOT NULL AND TRIM(Competency_Group) NOT = ''
)STG
LEFT OUTER JOIN EDWHR_STAGING.Ref_Competency_Group_Wrk TGT
ON SUBSTR(STG.Competency_Group_Desc,1,100) = TGT.Competency_Group_Desc

WHERE TGT.Competency_Group_Desc IS NULL
) Y
;


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
/*  Collect Statistics on the WRK Table    */


CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_Competency_Group_WRK');


Insert into EDWHR.Ref_Competency_Group
 ( 
Competency_Group_Id,
Competency_Group_Desc,
Source_System_Code,
DW_LAST_UPDATE_DATE_TIME

 )
select Row_Number() Over ( ORDER BY WRK.Competency_Group_Id) + (SELECT Coalesce(Max(Competency_Group_Id),0) FROM EDWHR.Ref_Competency_Group) AS Competency_Group_Id,
Trim(Competency_Group_Desc),
WRK.SOURCE_SYSTEM_CODE ,
current_timestamp(0) as dw_last_update_date_time
FROM EDWHR_STAGING.Ref_Competency_Group_Wrk WRK
where WRK.Competency_Group_Desc not in (sel Competency_Group_Desc from EDWHR.Ref_Competency_Group); 


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL dbadmin_procs.collect_stats_table ('$NCR_TGT_SCHEMA','Ref_Competency_Group');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF


