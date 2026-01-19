select 'J_IM_EDW_Staff_PM_Users' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
EDWIM_STAGING.Staff_PM_Users)A;