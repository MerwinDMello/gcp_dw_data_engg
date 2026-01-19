#####################################################################################
#                                               					#
#   SCRIPT NAME     - HDW_TMS_Ref_Performance_Period.SQL                  	
#   Job NAME    	- J_HDW_TMS_Ref_Performance_Period                        	#
#   TARGET TABLE    - $NCR_TGT_SCHEMA.Ref_Performance_Period                     	#
#   Developer   	- Julia Kim                                 			#
#   Version 		- 1.0 - Initial RELEASE                     			#
#   Description 	- The SCRIPT loads the TARGET TABLE WITH NEW records		#
#####################################################################################


bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Performance_Period;' FOR SESSION;

/* Load Work Table with working Data */







DELETE FROM EDWHR_Staging.Ref_Performance_Period_WRK;


INSERT INTO EDWHR_Staging.Ref_Performance_Period_WRK 
(
Review_Period_Id,
Review_Period_Desc,
Source_System_Code,
DW_Last_Update_Date_Time
)
SELECT 
(SELECT (COALESCE(MAX(Review_Period_Id),0)) FROM EDWHR_BASE_VIEWS.Ref_Performance_Period) + 
ROW_NUMBER() OVER (ORDER BY Y.Review_Period_Desc) AS Review_Period_Id
, Y.Review_Period_Desc
, 'M' AS SOURCE_SYSTEM_CODE
, CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM (
SEL STG.Review_Period_Desc
FROM 
(   
SELECT 
DISTINCT(TRIM(Review_Period)) AS Review_Period_Desc
FROM EDWHR_STAGING.COMPETENCY_RATINGS_REPORT
WHERE TRIM(Review_Period) IS NOT NULL AND TRIM(Review_Period) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Rev_Period)) AS Review_Period_Desc
FROM EDWHR_STAGING.PERFORMANCE_RATINGS_REPORT
WHERE TRIM(Rev_Period) IS NOT NULL AND TRIM(Rev_Period) NOT = ''

UNION

SELECT 
DISTINCT(TRIM(Review_Period)) AS Review_Period_Desc
FROM EDWHR_STAGING.Employee_Perf_Goals
WHERE TRIM(Review_Period) IS NOT NULL AND TRIM(Review_Period) NOT = ''
) STG

LEFT OUTER JOIN EDWHR_Staging.Ref_Performance_Period_WRK  TGT
ON SUBSTR(STG.Review_Period_Desc,1,100) = TGT.Review_Period_Desc

WHERE TGT.Review_Period_Desc IS NULL
) Y
;


.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
/*  Collect Statistics on the WRK Table    */

CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_Performance_Period_WRK');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


Insert into EDWHR.Ref_Performance_Period ( 
Review_Period_Id,
Review_Period_Desc,
Source_System_Code,
DW_Last_Update_Date_Time
 )
select Row_Number() Over ( ORDER BY WRK.Review_Period_Id) + (SELECT Coalesce(Max(Review_Period_Id),0) FROM EDWHR.Ref_Performance_Period) AS Review_Period_Id,
Trim(Review_Period_Desc) ,
WRK.SOURCE_SYSTEM_CODE ,
current_timestamp(0) as dw_last_update_date_time
FROM EDWHR_Staging.Ref_Performance_Period_WRK  WRK
where  WRK.Review_Period_Desc not in (sel Review_Period_Desc from EDWHR.Ref_Performance_Period); 



/*  Collect Statistics on the Target Table    */

CALL dbadmin_procs.collect_stats_table ('$NCR_TGT_SCHEMA','Ref_Performance_Period');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF

