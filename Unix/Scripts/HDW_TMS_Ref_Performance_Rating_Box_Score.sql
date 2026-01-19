#####################################################################################
#                                               					#
#   SCRIPT NAME     - HDW_TMS_Ref_Performance_Rating_Box_Score.sql                  	
#   Job NAME    	- J_HDW_TMS_Ref_Performance_Rating_Box_Score                        	#
#   TARGET TABLE    - $NCR_TGT_SCHEMA.Ref_Performance_Rating_Box_Score                    	#
#   Developer   	- Nathan Butler 
#					- 20180316                               			#
#   Version 		- 1.0 - Initial RELEASE                     			#
#   Description 	- The SCRIPT loads the TARGET TABLE WITH NEW records		#
#####################################################################################

bteq << EOF > $1;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'App=HRDM_ETL; Job=J_HDW_TMS_Ref_Performance_Rating_Box_Score;' FOR SESSION;

/* Load Work Table with working Data */
DELETE FROM $NCR_STG_SCHEMA.Ref_Performance_Rating_Box_Score_WRK;

INSERT INTO $NCR_STG_SCHEMA.Ref_Performance_Rating_Box_Score_WRK
(
Performance_Rating_Id
, Performance_Potential_Id
, Overall_Performance_Rating_Desc
, Performance_Potential_Desc
, Box_Score_Num
, Box_Score_Desc
, Box_Score_Group_Desc
, Source_System_Code
, DW_LAST_UPDATE_DATE_TIME
)
SELECT CAST(R.Performance_Rating_Id AS INTEGER ) AS Performance_Rating_Id
	,CAST(R1.Probability_Potential_Id AS INTEGER) AS Performance_Potential_Id
	,HR.Ovrall_Calibrated_Perf_Rvw AS Overall_Performance_Rating_Desc 
	,HR.TMS_Box_Potential AS Performance_Potential_Desc 
	,CAST(CASE WHEN ISNUMERIC(HR.Box_Score) = 0 
	THEN HR.Box_Score ELSE NULL END AS INTEGER) 
	AS Box_Score_Num 
	,HR.Box_Description AS Box_Score_Desc 
	,HR.Box_Bracket AS Box_Score_Group_Desc 
	,'M' AS Source_System_Code
	,current_timestamp(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.Map_TMS_Box HR
	INNER JOIN EDWHR.Ref_Performance_Rating R
	ON HR.Ovrall_Calibrated_Perf_Rvw = 
	R.Performance_Rating_Desc
	INNER JOIN EDWHR.Ref_Probability_Potential R1
	ON HR.TMS_Box_Potential = 
	R1.Probability_Potential_Desc
;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

/*Collect Stats on WRK table*/
CALL dbadmin_procs.collect_stats_table ('$NCR_STG_SCHEMA','Ref_Performance_Rating_Box_Score_WRK');

/*Core table load*/
INSERT INTO $NCR_TGT_SCHEMA.Ref_Performance_Rating_Box_Score
(
Performance_Rating_Id
, Performance_Potential_Id
, Overall_Performance_Rating_Desc
, Performance_Potential_Desc
, Box_Score_Num
, Box_Score_Desc
, Box_Score_Group_Desc
, Source_System_Code
, DW_LAST_UPDATE_DATE_TIME
)
SELECT 
W.Performance_Rating_Id
, W.Performance_Potential_Id
, W.Overall_Performance_Rating_Desc
, W.Performance_Potential_Desc
, W.Box_Score_Num
, W.Box_Score_Desc
, W.Box_Score_Group_Desc
, W.Source_System_Code
, W.DW_LAST_UPDATE_DATE_TIME
FROM $NCR_STG_SCHEMA.Ref_Performance_Rating_Box_Score_WRK W
LEFT JOIN $NCR_TGT_SCHEMA.Ref_Performance_Rating_Box_Score C
	on  W.Performance_Rating_Id = C. Performance_Rating_Id 
	AND W.Performance_Potential_Id = C.Performance_Potential_Id
where C.Performance_Rating_Id IS null AND C.Performance_Potential_Id IS NULL
;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL dbadmin_procs.collect_stats_table ('$NCR_TGT_SCHEMA','Ref_Performance_Rating_Box_Score');

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

.Logoff;

.EXIT

EOF