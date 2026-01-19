export Job_Name='J_IM_PK_Instance_Facility_Xwalk'
export JOBNAME='J_IM_PK_Instance_Facility_Xwalk'


export AC_EXP_SQL_STATEMENT="select 'J_IM_PK_Instance_Facility_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
(
SELECT DISTINCT
t3.IM_Domain_Id
,t2.Company_Code
,t2.Coid

FROM
EDWIM_BASE_VIEWS.Ref_PK_Data_Base_Instance t1
INNER JOIN EDWIM_BASE_VIEWS.PK_Encounter t2
ON t1.PK_Database_Instance_Sid = t2.PK_Database_Instance_Sid
INNER JOIN EDWIM_BASE_VIEWS.Ref_IM_Domain t3
ON  t1.PK_Database_Instance_Code = t3.IM_Domain_Name
AND t3.Application_System_Id = 8)X)A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_PK_Instance_Facility_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 edwim_staging.PK_Instance_Facility_Xwalk)A;"