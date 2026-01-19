select 'J_IM_Platform_Domain_Xwalk' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 edwim.Platform_Domain_Xwalk)A;