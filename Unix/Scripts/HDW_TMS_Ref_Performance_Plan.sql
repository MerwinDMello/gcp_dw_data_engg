#####################################################################################
#                                               					#
#   SCRIPT NAME     - HDW_TMS_Ref_Performance_Plan.sql                  	
#   Job NAME    	- J_HDW_TMS_Ref_Performance_Plan                        	#
#   TARGET TABLE    - $NCR_TGT_SCHEMA.Ref_Performance_Plan                     	#
#   Developer   	- Nathan Butler 
#					- 20180316                               			#
#   Version 		- 1.0 - Initial RELEASE                     			#
#   Description 	- The SCRIPT loads the TARGET TABLE WITH NEW records		#
#####################################################################################

bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Performance_Plan;' FOR SESSION;

/* Load Work Table with working Data */

DELETE FROM EDWHR_STAGING.Ref_Performance_Plan_WRK;

INSERT INTO EDWHR_STAGING.Ref_Performance_Plan_WRK
SELECT DISTINCT 
COALESCE(A.Performance_Plan_Id,'U')
, 'M' AS Source_System_Code
, CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_TIME
FROM
	(
		SELECT DISTINCT TRIM(Plan_Name) AS Performance_Plan_Id
		FROM EDWHR_STAGING.EMPLOYEE_PERF_GOALS
		UNION ALL
		SELECT DISTINCT TRIM(Plan_Name) AS Performance_Plan_Id
		FROM EDWHR_STAGING.PERFORMANCE_RATINGS_REPORT
		UNION ALL 
		SELECT DISTINCT TRIM(Plan_Name) AS Performance_Plan_Id
		FROM EDWHR_STAGING.COMPETENCY_RATINGS_REPORT
	) A
	;
INSERT INTO EDWHR.Ref_Performance_Plan
(
  Performance_Plan_Id
, Performance_Plan_Desc
, Source_System_Code
, DW_Last_Update_Date_Time
)

SELECT 
CAST(CAST(
(
SELECT COALESCE(MAX(Performance_Plan_Id),0)
FROM EDWHR.Ref_Performance_Plan
)
+ ROW_NUMBER() OVER (ORDER BY WRK.Performance_Plan_Desc) AS INT) AS CHAR(10))
AS
  Performance_Plan_Id
, Performance_Plan_Desc
, WRK.Source_System_Code
, WRK.DW_LAST_UPDATE_TIME
FROM EDWHR_STAGING.Ref_Performance_Plan_WRK WRK
WHERE WRK.Performance_Plan_Desc
NOT IN (SELECT Performance_Plan_Desc FROM EDWHR.Ref_Performance_Plan)
;



