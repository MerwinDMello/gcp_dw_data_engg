export Job_Name='J_IM_PK_User_Archive'
export JOBNAME='J_IM_PK_User_Archive'


export AC_EXP_SQL_STATEMENT="select 'J_IM_PK_User_Archive' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
(
SELECT
t2.IM_Domain_Id
,t1.PK_User_Id
,t2.eSAF_Activity_Date
,t1.PK_Database_Instance_Id
,t1.PK_User_Last_Activity_Date
,t1.Source_System_Code
,t1.DW_Last_Update_Date_Time
FROM 
EDWIM_BASE_VIEWS.PK_User t1
INNER JOIN
EDWIM_BASE_VIEWS.IM_Person_Activity t2
ON t1.PK_User_Id = t2.IM_Person_User_Id
QUALIFY ROW_NUMBER() OVER (PARTITION BY t2.IM_Domain_Id, t1.PK_User_Id
                                           ORDER BY  PK_User_Last_Activity_Date DESC) = 1)X)A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_PK_User_Archive' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 edwim.PK_User_Archive)A;"