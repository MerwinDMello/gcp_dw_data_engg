SELECT
Claim_ID,
Phy_ID as Phys_Code,
Phy_First_Name as Phys_First_Name,
Phy_Last_Name as Phys_Last_Name,
Phy_Qual_ID as Phys_Qual_Code,
phy_Taxonomy_Code as Phys_Taxonomy_Code,
Phy_Type_ID as Phys_Type_Code,
NULL as DW_Last_Update_Date_Time,
NULL as Source_System_Code
FROM ClaimsConnectDB.dbo.Fact_Claim_Physician With (Nolock)