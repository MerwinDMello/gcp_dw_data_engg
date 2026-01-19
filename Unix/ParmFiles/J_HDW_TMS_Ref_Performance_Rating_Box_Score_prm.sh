export JOBNAME='J_HDW_TMS_Ref_Performance_Rating_Box_Score'

######Expected Statment#######

export AC_EXP_SQL_STATEMENT="SELECT '${JOBNAME}' ||','|| CAST(COUNT(*) AS VARCHAR(50))  ||','AS SOURCE_STRING 

FROM 
(SELECT 
Performance_Rating_Id AS Rating_SID
, Performance_Potential_Id AS Performance_Potential_SID
, Overall_Performance_Rating_Desc
, Performance_Potential_Desc
, Box_Score_Num
, Box_Score_Desc
, Box_Score_Group_Desc
, Source_System_Code
, DW_LAST_UPDATE_DATE_TIME
FROM $NCR_STG_SCHEMA.Ref_Performance_Rating_Box_Score_WRK) STG
"
######Actual Statment#########

export AC_ACT_SQL_STATEMENT="select '$JOBNAME' ||','|| cast(count(*) as varchar(50))  ||','as SOURCE_STRING 
FROM $NCR_TGT_SCHEMA.Ref_Performance_Rating_Box_Score
"