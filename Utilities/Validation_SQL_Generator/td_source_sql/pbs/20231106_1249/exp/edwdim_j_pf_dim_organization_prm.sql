
SELECT 'PBMOR100-020'||','||Trim(Count(*))||',' AS Source_String FROM 
( SELECT 
Coid, ASO_BSO_Storage_Code, Org_Name_Parent, Org_Name_Child, Org_Alias_Name, Org_Coid_Alias_Name, Alias_Table_Name, Consolidation_Code, Storage_Code, Two_Pass_Calc_Code, Formula_Text, Member_Solve_Order_Num, Org_Level_UDA_Name, Org_Hier_Name
FROM EDWPbs_STAGING.Dim_Org_Stg_Mthy 
UNION 
SELECT
 Coid, ASO_BSO_Storage_Code, Org_Name_Parent, Org_Name_Child, Org_Alias_Name, Org_Coid_Alias_Name, Alias_Table_Name, Consolidation_Code, Storage_Code, Two_Pass_Calc_Code, Formula_Text, Member_Solve_Order_Num, Org_Level_UDA_Name, Org_Hier_Name
FROM  EDWPBS_STAGING.Dim_Org_Stg_Mthy_PF_T3185_DND 
UNION 
SELECT 
 Coid, ASO_BSO_Storage_Code, Org_Name_Parent, Org_Name_Child, Org_Alias_Name, Org_Coid_Alias_Name, Alias_Table_Name, Consolidation_Code, Storage_Code, Two_Pass_Calc_Code, Formula_Text, Member_Solve_Order_Num, Org_Level_UDA_Name, Org_Hier_Name
FROM  EDWPBS_STAGING.dim_esb_organization_HomeHealth_DND
) AA