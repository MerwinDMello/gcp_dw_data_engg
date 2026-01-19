SELECT
Provider_Addr1 as Pay_To_Provider_Addr1,
Provider_Addr2 as Pay_To_Provider_Addr2,
Provider_City as Pay_To_Provider_City,
Provider_Name as Pay_To_Provider_Name,
Pay_To_Provider_SID,
Provider_St as Pay_To_Provider_St,
Provider_Zip Pay_To_Provider_Zip_Cd,
DW_Last_Update_Date_Time,
Source_System_Code
FROM
ClaimsConnectDB.dbo.Dw_Pay_To_Provider WITH (nolock);