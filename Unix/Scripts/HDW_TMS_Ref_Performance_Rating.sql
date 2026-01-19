
#####################################################################################
#                                               					#
#   SCRIPT NAME     - HDW_TMS_Ref_Performance_Rating.SQL                  	
#   Job NAME    	- J_HDW_TMS_Ref_Performance_Rating                       	#
#   TARGET TABLE    - $NCR_TGT_SCHEMA.Ref_Performance_Rating                    	#
#   Developer   	- Julia Kim                                 			#
#   Version 		- 1.0 - Initial RELEASE                     			#
#   Description 	- The SCRIPT loads the TARGET TABLE WITH NEW records		#
#####################################################################################


bteq << EOF > $1;


.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Performance_Rating;' FOR SESSION;

/* Load Work Table with working Data */







DELETE FROM EDWHR_Staging.Ref_Performance_Rating_Wrk;


INSERT INTO EDWHR_STAGING.Ref_Performance_Rating_Wrk
(
Performance_Rating_Id,
Performance_Rating_Desc,
Source_System_Code,
DW_LAST_UPDATE_DATE_TIME
)
SELECT 
(SELECT (COALESCE(MAX(Performance_Rating_Id),0)) FROM EDWHR_BASE_VIEWS.Ref_Performance_Rating) + 
ROW_NUMBER() OVER (ORDER BY Y.Performance_Rating_Desc) AS Performance_Rating_Id
,Y.Performance_Rating_Desc
,'M' AS SOURCE_SYSTEM_CODE
,current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME

FROM (
SEL STG.Performance_Rating_Desc
FROM 
(   
SELECT 
DISTINCT(TRIM(Calibrate_Overall_Perf_Rating)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Employee_Info
WHERE TRIM(Calibrate_Overall_Perf_Rating) IS NOT NULL AND TRIM(Calibrate_Overall_Perf_Rating) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Overall_Performance_Rating)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Employee_Info
WHERE TRIM(Overall_Performance_Rating) IS NOT NULL AND TRIM(Overall_Performance_Rating) NOT = ''

UNION 

/*SELECT 
DISTINCT(TRIM(Summary_Competency_Rating)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Employee_Info
WHERE TRIM(Summary_Competency_Rating) IS NOT NULL AND TRIM(Summary_Competency_Rating) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Summary_Goal_Rating)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Employee_Info
WHERE TRIM(Summary_Goal_Rating) IS NOT NULL AND TRIM(Summary_Goal_Rating) NOT = ''

UNION */

SELECT 
DISTINCT(TRIM(Emp_Goal_Rating)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Employee_Perf_Goals
WHERE TRIM(Emp_Goal_Rating) IS NOT NULL AND TRIM(Emp_Goal_Rating) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Mgr_Goal_Rating)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Employee_Perf_Goals
WHERE TRIM(Mgr_Goal_Rating) IS NOT NULL AND TRIM(Mgr_Goal_Rating) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Employee_Rating_Scale_Value)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Competency_Ratings_Report
WHERE TRIM(Employee_Rating_Scale_Value) IS NOT NULL AND TRIM(Employee_Rating_Scale_Value) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Manager_Rating_Scale_Value)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Competency_Ratings_Report
WHERE TRIM(Manager_Rating_Scale_Value) IS NOT NULL AND TRIM(Manager_Rating_Scale_Value) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM( Emp_Perf_Rat_Scale_Val )) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Performance_Ratings_Report
WHERE TRIM( Emp_Perf_Rat_Scale_Val ) IS NOT NULL AND TRIM( Emp_Perf_Rat_Scale_Val ) NOT = ''


UNION 

SELECT 
DISTINCT(TRIM( Perf_Rat_Scale_Val )) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Performance_Ratings_Report
WHERE TRIM( Perf_Rat_Scale_Val ) IS NOT NULL AND TRIM( Perf_Rat_Scale_Val ) NOT = ''


UNION 

SELECT 
DISTINCT(TRIM( Emp_Smry_Comp_Rat_Scale_Val )) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Performance_Ratings_Report
WHERE TRIM( Emp_Smry_Comp_Rat_Scale_Val ) IS NOT NULL AND TRIM( Emp_Smry_Comp_Rat_Scale_Val ) NOT = ''


UNION 

SELECT 
DISTINCT(TRIM( Sumry_Comp_Rat_Scale_Val )) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Performance_Ratings_Report
WHERE TRIM( Sumry_Comp_Rat_Scale_Val ) IS NOT NULL AND TRIM( Sumry_Comp_Rat_Scale_Val ) NOT = ''


UNION 

SELECT 
DISTINCT(TRIM( Emp_Smry_Goal_Rat_Scale_Val )) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Performance_Ratings_Report
WHERE TRIM( Emp_Smry_Goal_Rat_Scale_Val ) IS NOT NULL AND TRIM( Emp_Smry_Goal_Rat_Scale_Val ) NOT = ''

UNION 

SELECT 
DISTINCT(TRIM(Smry_Goal_Rat_Scale_Val)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Performance_Ratings_Report
WHERE TRIM(Smry_Goal_Rat_Scale_Val) IS NOT NULL AND TRIM(Smry_Goal_Rat_Scale_Val) NOT = ''

UNION

SELECT
DISTINCT(TRIM(Ovrall_Calibrated_Perf_Rvw)) AS Performance_Rating_Desc
FROM EDWHR_STAGING.Map_TMS_Box
WHERE TRIM(Ovrall_Calibrated_Perf_Rvw) IS NOT NULL AND TRIM(Ovrall_Calibrated_Perf_Rvw) NOT = ''


) STG

LEFT OUTER JOIN EDWHR_STAGING.Ref_Performance_Rating_Wrk  TGT
ON SUBSTR(STG.Performance_Rating_Desc,1,100) = TGT.Performance_Rating_Desc

WHERE TGT.Performance_Rating_Desc IS NULL
) Y
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;
/*  Collect Statistics on the WRK Table    */


CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_Performance_Rating_Wrk');

/*delete the value of '---------'*/

delete from EDWHR_Staging.Ref_Performance_Rating_Wrk 
where Performance_Rating_Desc = '----------';
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

Insert into EDWHR.Ref_Performance_Rating ( 
Performance_Rating_Id,
Performance_Rating_Desc,
Source_System_Code,
DW_LAST_UPDATE_DATE_TIME

 )
select Row_Number() Over ( ORDER BY WRK.Performance_Rating_Id) + (SELECT Coalesce(Max(Performance_Rating_Id),0) FROM EDWHR.Ref_Performance_Rating) AS Performance_Rating_Id,
Trim(Performance_Rating_Desc),
WRK.SOURCE_SYSTEM_CODE ,
current_timestamp(0) as dw_last_update_date_time
FROM EDWHR_Staging.Ref_Performance_Rating_Wrk  WRK
where WRK.Performance_Rating_Desc not in (sel Performance_Rating_Desc from EDWHR.Ref_Performance_Rating); 

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*  Collect Statistics on the Target Table    */

CALL dbadmin_procs.collect_stats_table ('$NCR_TGT_SCHEMA','Ref_Performance_Rating');
 
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF











