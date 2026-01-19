select 'J_IM_Platform_Domain_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
(
SELECT DISTINCT
t1.IM_Domain_Id
,COALESCE(t2.IM_Domain_Id, 0) AS MT_Domain_Id
,COALESCE(t3.IM_Domain_Id, 0) AS PK_Domain_Id
FROM
EDWIM_STAGING.HPF_Instance_Facility_Xwalk t1
LEFT JOIN
EDWIM_STAGING.MT_Instance_Facility_Xwalk t2
ON t1.Company_Code = t2.Company_Code
AND t1.Coid = t2.Coid
LEFT JOIN
EDWIM_STAGING.PK_Instance_Facility_Xwalk t3
ON t1.Company_Code = t3.Company_Code
AND t1.Coid = t3.Coid)X)A;