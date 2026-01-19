select 'J_IM_HPF_Instance_Facility_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 EDWIM_staging.HPF_Instance_Facility_Xwalk)A;