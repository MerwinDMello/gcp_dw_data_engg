select 'J_IM_PK_User_Archive' ||','||cast(zeroifnull(A.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
 edwim.PK_User_Archive)A;