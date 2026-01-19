select 'J_IM_Cactus_Provider_Archive' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 edwim.Cactus_Provider_Archive)A;