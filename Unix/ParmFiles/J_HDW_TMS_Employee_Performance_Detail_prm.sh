####Variable Declarations####
export JOBNAME='J_HDW_TMS_Employee_Performance_Detail'

####Expected Parameter####
export AC_EXP_SQL_STATEMENT="SELECT '$JOBNAME' || ',' || CAST(COUNT(*) AS VARCHAR(50)) || ',' AS SOURCE_STRING 
FROM (
SELECT ROW_NUMBER() OVER (ORDER BY STG.Evaluation_Record_ID) AS Employee_Performance_SID
FROM 
EDWHR_STAGING.PERFORMANCE_RATINGS_REPORT STG
    INNER JOIN EDWHR.Employee_Talent_Profile Tal
    ON CAST(COALESCE(STG.Employee_ID, 0) AS BIGINT) = Tal.Employee_Num
    WHERE SUBSTR(Employee_ID, 1, 1) IN ('1','2','3','4','5','6','7','8','9','0')
    AND SUBSTR(Employee_ID, 5, 1) IN ('1','2','3','4','5','6','7','8','9','0')
    AND STG.Evaluation_Record_ID IS NOT NULL AND Valid_From_Date IS NOT NULL
    AND Tal.Lawson_Company_Num IS NOT NULL AND Tal.Process_Level_Code IS NOT NULL
) SRC


"


####Actual Parameter####
export AC_ACT_SQL_STATEMENT="SELECT '$JOBNAME' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING FROM 
EDWHR.Employee_Performance_Detail
WHERE Valid_To_Date = CAST('9999-12-31' AS DATE)

"