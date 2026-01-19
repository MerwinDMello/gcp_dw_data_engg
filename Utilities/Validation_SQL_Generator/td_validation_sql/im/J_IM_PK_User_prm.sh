export Job_Name='J_IM_PK_User'
export JOBNAME='J_IM_PK_User'


export AC_EXP_SQL_STATEMENT="select 'J_IM_PK_User' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
(
SELECT 
t4.IM_Domain_Id
,CAST(t1.PK_User_Id_3_4 AS VARCHAR(20)) AS PK_User_Id
,t1.PK_Database_Instance_Sid
,CAST(t1.Eff_From_Date_Time AS DATE) AS PK_User_Last_Activity_Date

FROM
 EDWIM_BASE_VIEWS.PK_Login_Information t1
INNER JOIN EDWIM_BASE_VIEWS.Junc_PK_User_Access_Level t2
ON t1.PK_Database_Instance_Sid = t2.PK_Database_Instance_Sid
AND t1.PK_Person_Id = t2.PK_Person_Id
AND t2.PK_Access_Level_Id = 3

INNER JOIN EDWIM_BASE_VIEWS.Ref_PK_Data_Base_Instance t3
ON t1.PK_Database_Instance_Sid =t3.PK_Database_Instance_Sid
INNER JOIN EDWIM_BASE_VIEWS.Ref_IM_Domain t4
ON t4.Application_System_Id = 8
AND 
 t3.PK_Database_Instance_Code =t4.IM_Domain_Name
WHERE  CHAR_LENGTH(TRIM(t1.PK_User_Id_3_4)) = 7 
AND REGEXP_INSTR(SUBSTR(TRIM(t1.PK_User_Id_3_4) ,4 ,4), '[A-Za-z_]') = 0
AND REGEXP_INSTR(SUBSTR(TRIM(t1.PK_User_Id_3_4) ,1 ,3), '[0-9_]') = 0 
AND CAST(t1.Eff_From_Date_Time AS DATE) >= CURRENT_DATE - 365

QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.PK_Database_Instance_Sid, t1.PK_User_Id_3_4
                                                                               ORDER BY t1.Eff_From_Date_Time DESC) = 1 )X  )A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_PK_User' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 EDWIM.PK_User)A;"