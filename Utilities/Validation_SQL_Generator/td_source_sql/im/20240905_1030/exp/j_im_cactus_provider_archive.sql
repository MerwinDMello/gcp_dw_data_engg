select 'J_IM_Cactus_Provider_Archive' ||','||cast(zeroifnull(F.counts) as varchar(20)) ||',' AS SOURCE_STRING from 
(
 SELECT COUNT(*) as counts FROM
(
SELECT
T2.IM_Domain_Id,
T1.Cactus_Provider_User_Id,
T2.eSAF_Activity_Date,
T1.Cactus_Provider_Src_Sys_Key,
T1.Cactus_Provider_NPI,
T1.Fac_Asgn_Stts_Sid,
T1.Fac_Asgn_Stts_Src_Sys_Key,
T1.Cactus_Provider_Cat_Sid,
T1.Cactus_Provider_Cat_Src_Sys_Key,
T1.Cactus_Provider_First_Name,
T1.Cactus_Provider_Last_Name,
T1.Cactus_Provider_Middle_Name,
T1.Cactus_Provider_Activity_Exempt_Sw,
T1.Source_System_Code,
T1.DW_Last_Update_Date_Time
FROM 
EDWIM_BASE_VIEWS.Cactus_Provider T1
INNER JOIN
EDWIM_BASE_VIEWS.IM_Person_Activity T2
ON T1.Cactus_Provider_User_Id= T2.IM_Person_User_Id
AND T1.IM_Domain_Id = T2.IM_Domain_Id
QUALIFY ROW_NUMBER() OVER (PARTITION BY T2.IM_Domain_Id, T1.Cactus_Provider_User_Id ORDER BY  Fac_Asgn_Stts_Sid DESC) = 1 )A
)  F