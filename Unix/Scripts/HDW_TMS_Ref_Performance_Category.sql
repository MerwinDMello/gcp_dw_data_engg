#########################################################################
#    Target Table:    <<EDWHR_Staging.Employee_Perf_Goals>>             #
# This script loads data from EDWHR.Ref_Performance_Category            #
# and inserts into the reference table using BTEQ                       #
#                                                                       #
#########################################################################
# Change Control:                                                       #
#                                                                       #
# Date                                     INITIAL RELEASE              #
# 03/02/2018 Skylar Youngblood              Initial version             #
#########################################################################

bteq << EOF >> $1;

.SET ERROROUT STDOUT;

.RUN FILE $LOGONDIR/HDW_AC;

SET QUERY_BAND = 'APP = HRDM_ETL; JOB = J_HDW_TMS_Ref_Performance_Category;' FOR SESSION;

BT;

MERGE INTO EDWHR.Ref_Performance_Category TGT

USING (
SELECT 
ROW_NUMBER() OVER(ORDER BY Goal_Category DESC) as ID,
STG.Goal_Category
FROM 

(SELECT 
Goal_Category,
ROW_NUMBER() OVER (PARTITION BY Goal_Category ORDER BY Goal_Category) AS RowNum

FROM EDWHR_Staging.Employee_Perf_Goals) STG

WHERE STG.Goal_Category IS NOT NULL 
AND STG.RowNum = 1 
    ) SRC


ON TGT.Performance_Category_Id = SRC.ID

WHEN MATCHED THEN
    UPDATE  SET
        Performance_Category_Desc=SRC.Goal_Category,
        Source_System_Code='M',
        DW_Last_Update_Date_Time=Current_Timestamp(0)
        
WHEN NOT MATCHED THEN
    INSERT (
        SRC.ID,
        SRC.Goal_Category,
        'M',
        CURRENT_TIMESTAMP(0)
        )

;
    
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;


ET;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE;

CALL DBADMIN_PROCS.COLLECT_STATS_TABLE ('EDWHR','Ref_Performance_Category');

.QUIT;

.EXIT