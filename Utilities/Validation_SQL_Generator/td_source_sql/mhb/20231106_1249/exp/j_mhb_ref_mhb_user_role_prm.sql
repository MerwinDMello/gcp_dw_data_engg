SELECT 'J_MHB_Ref_MHB_User_Role'||','||CAST(Count(*) AS VARCHAR (20))||','
AS SOURCE_STRING FROM
(
select * from 
(sel distinct MHB_User_Role_Desc
 from EDWCI_Staging.User_Role_Stg)X
 where X.MHB_User_Role_Desc not in (sel MHB_User_Role_Desc from Edwci.Ref_MHB_User_Role  )
) Q