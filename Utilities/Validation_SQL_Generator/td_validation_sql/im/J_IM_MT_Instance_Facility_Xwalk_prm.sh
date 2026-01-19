export Job_Name='J_IM_MT_Instance_Facility_Xwalk'
export JOBNAME='J_IM_MT_Instance_Facility_Xwalk'


export AC_EXP_SQL_STATEMENT="select 'J_IM_MT_Instance_Facility_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
(
SELECT
IM_Domain_Id 
,Company_Code 
,Coid
FROM (

SELECT	distinct
t3.IM_Domain_Id
,t1.Company_Code
,t1.Coid
,
CASE	
        WHEN t1.Coid = '31768'
        THEN 6
        ELSE 5 
END	AS Appl_System_Id
FROM	
EDW_PUB_VIEWS.Clinical_Facility t1
INNER JOIN	EDWIM_BASE_VIEWS.Ref_IM_Domain t3
	ON	t1.Network_Mnemonic_CS = t3.IM_Domain_Name
	AND	t3.Application_System_Id = Appl_System_Id) x
group by 1,2,3)X)A;"


export AC_ACT_SQL_STATEMENT="select 'J_IM_MT_Instance_Facility_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 edwim_staging.MT_Instance_Facility_Xwalk)A;"