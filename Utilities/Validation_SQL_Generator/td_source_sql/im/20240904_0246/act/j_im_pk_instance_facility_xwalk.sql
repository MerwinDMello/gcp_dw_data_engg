select 'J_IM_PK_Instance_Facility_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 edwim_staging.PK_Instance_Facility_Xwalk)A;