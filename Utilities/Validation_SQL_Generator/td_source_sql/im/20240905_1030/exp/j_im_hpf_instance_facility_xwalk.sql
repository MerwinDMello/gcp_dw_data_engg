select 'J_IM_HPF_Instance_Facility_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
(
SELECT DISTINCT
t3.IM_Domain_Id
,t2.Company_Code
,t2.Coid
FROM
EDWIM_BASE_VIEWS.Document_Work_Flow_Instance t1
INNER JOIN 
EDWIM_BASE_VIEWS.Deficiency_Audit t2
ON t1.Instance_DW_Id = t2.Instance_DW_Id
INNER JOIN
EDWIM_BASE_VIEWS.Ref_IM_Domain t3
ON t1.Instance_Connection_String = t3.IM_Domain_Name
AND t3.Application_System_Id = 3)X)A;