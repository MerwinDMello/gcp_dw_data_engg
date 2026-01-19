#####################################################################################
#                                               					#
#   SCRIPT NAME     - HDW_TMS_Ref_Probability_Potential.SQL                  	
#   Job NAME    	- J_HDW_TMS_Ref_Probability_Potential                       	#
#   TARGET TABLE    - $NCR_TGT_SCHEMA.Ref_Probability_Potential                    	#
#   Developer   	- Julia Kim                                 			#
#   Version 		- 1.0 - Initial RELEASE                     			#
#   Description 	- The SCRIPT loads the TARGET TABLE WITH NEW records		#
#####################################################################################


bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Probability_Potential;' FOR SESSION;

/* Load Work Table with working Data */


DELETE FROM EDWHR_Staging.Ref_Probability_Potential_Wrk;


INSERT INTO EDWHR_Staging.Ref_Probability_Potential_Wrk

(
Probability_Potential_Id,
Probability_Potential_Desc,
Source_System_Code,
DW_Last_Update_Date_Time
)
SELECT 
(SELECT (COALESCE(MAX(Probability_Potential_Id),0)) FROM EDWHR_BASE_VIEWS.Ref_Probability_Potential) + 
ROW_NUMBER() OVER (ORDER BY Y.Probability_Potential_Desc) AS Probability_Potential_Id
, Y.Probability_Potential_Desc
, 'M' AS SOURCE_SYSTEM_CODE
, CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM (
SEL STG.Probability_Potential_Desc
FROM 
(   
SELECT 
DISTINCT(Trim(Potential)) AS Probability_Potential_Desc
FROM EDWHR_STAGING.Employee_Info
WHERE TRIM(Potential) IS NOT NULL 
AND TRIM(Potential) NOT = ''

UNION 

SELECT 
DISTINCT(Trim(Employee_Promote_Interest)) AS Probability_Potential_Desc
FROM EDWHR_STAGING.Employee_Info
WHERE TRIM(Employee_Promote_Interest) IS NOT NULL 
AND TRIM(Employee_Promote_Interest) NOT = ''

UNION 


SELECT 
DISTINCT(TRIM(Flight_Risk)) AS Probability_Potential_Desc
FROM EDWHR_STAGING.Employee_Info
WHERE TRIM(Flight_Risk) IS NOT NULL 
AND TRIM(Flight_Risk) NOT = ''


UNION

SELECT 
DISTINCT(Trim(Priority)) AS Probability_Potential_Desc
FROM EDWHR_STAGING.DEVELOPMENT_ACTIVITIES_REPORT
WHERE TRIM(Priority)IS NOT NULL 
AND TRIM(Priority) NOT = ''

UNION

SELECT 
DISTINCT(Trim(TMS_Box_Potential)) AS Probability_Potential_Desc
FROM EDWHR_Staging.Map_TMS_Box
WHERE TRIM(TMS_Box_Potential)IS NOT NULL 
AND TRIM(TMS_Box_Potential) NOT = ''


 
) STG

LEFT OUTER JOIN EDWHR_Staging.Ref_Probability_Potential_WRK   TGT
ON SUBSTR(STG.Probability_Potential_Desc,1,100) = TGT.Probability_Potential_Desc

WHERE TGT.Probability_Potential_Desc IS NULL
) Y
;


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
/*  Collect Statistics on the WRK Table    */


CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_Performance_Rating_Wrk');

/*delete the value of '--------'*/

delete from EDWHR_Staging.Ref_Probability_Potential_Wrk
where Probability_Potential_Desc = '----------';
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


Insert into EDWHR.Ref_Probability_Potential
 ( 
Probability_Potential_Id,
Probability_Potential_Desc,
Source_System_Code,
DW_Last_Update_Date_Time

 )
select Row_Number() Over ( ORDER BY WRK.Probability_Potential_Id) + (SELECT Coalesce(Max(Probability_Potential_Id),0) FROM EDWHR.Ref_Probability_Potential) AS Probability_Potential_Id,
Trim(Probability_Potential_Desc),
WRK.SOURCE_SYSTEM_CODE ,
current_timestamp(0) as dw_last_update_date_time
FROM EDWHR_Staging.Ref_Probability_Potential_WRK WRK
where WRK.Probability_Potential_Desc not in (sel Probability_Potential_Desc from EDWHR.Ref_Probability_Potential); 

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL dbadmin_procs.collect_stats_table ('$NCR_TGT_SCHEMA','Ref_Probability_Potential');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF
