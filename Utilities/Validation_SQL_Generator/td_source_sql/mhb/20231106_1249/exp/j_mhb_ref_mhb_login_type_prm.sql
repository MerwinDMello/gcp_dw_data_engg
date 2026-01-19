SELECT 'J_MHB_Ref_MHB_Login_Type'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
sel * from (sel distinct Login_Type 
 from EDWCI_Staging.vwUserLogins)X
 where X.Login_Type not in (sel Login_Type_Desc from Edwci.Ref_MHB_Login_Type  )
) Q