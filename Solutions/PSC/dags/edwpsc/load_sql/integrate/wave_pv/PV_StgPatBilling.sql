
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgPatBilling AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgPatBilling AS source
ON target.PatBillingPK = source.PatBillingPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Pat_Num = source.Pat_Num,
 target.Payer_Num = source.Payer_Num,
 target.Bill_To = TRIM(source.Bill_To),
 target.Short_Name = TRIM(source.Short_Name),
 target.CLASS = TRIM(source.CLASS),
 target.TYPE = TRIM(source.TYPE),
 target.Priority = source.Priority,
 target.Pricing = TRIM(source.Pricing),
 target.WC_Pricing = TRIM(source.WC_Pricing),
 target.EPS_Pricing = TRIM(source.EPS_Pricing),
 target.PBC_Flag = TRIM(source.PBC_Flag),
 target.Percentage = source.Percentage,
 target.Assignment_Date = source.Assignment_Date,
 target.Assignment_Flag = TRIM(source.Assignment_Flag),
 target.Eft_Date = source.Eft_Date,
 target.Exp_Date = source.Exp_Date,
 target.Policy_Notes = TRIM(source.Policy_Notes),
 target.Svc_Copay = source.Svc_Copay,
 target.Med_Copay = source.Med_Copay,
 target.Member_ID = TRIM(source.Member_ID),
 target.Group_ID = TRIM(source.Group_ID),
 target.Auth_Num = TRIM(source.Auth_Num),
 target.Crg_Balance = source.Crg_Balance,
 target.Prim_Name = TRIM(source.Prim_Name),
 target.Prim_Address1 = TRIM(source.Prim_Address1),
 target.Prim_Address2 = TRIM(source.Prim_Address2),
 target.Prim_City = TRIM(source.Prim_City),
 target.Prim_State = TRIM(source.Prim_State),
 target.Prim_Zip = TRIM(source.Prim_Zip),
 target.Prim_Country = TRIM(source.Prim_Country),
 target.Prim_Attn = TRIM(source.Prim_Attn),
 target.Prim_Phone = TRIM(source.Prim_Phone),
 target.Prim_Phone_Ext = TRIM(source.Prim_Phone_Ext),
 target.Scnd_Name = TRIM(source.Scnd_Name),
 target.Scnd_Address1 = TRIM(source.Scnd_Address1),
 target.Scnd_Address2 = TRIM(source.Scnd_Address2),
 target.Scnd_City = TRIM(source.Scnd_City),
 target.Scnd_State = TRIM(source.Scnd_State),
 target.Scnd_Zip = TRIM(source.Scnd_Zip),
 target.Scnd_Country = TRIM(source.Scnd_Country),
 target.Scnd_Contact = TRIM(source.Scnd_Contact),
 target.Scnd_Phone = TRIM(source.Scnd_Phone),
 target.Scnd_Phone_Ext = TRIM(source.Scnd_Phone_Ext),
 target.Emp_Num = source.Emp_Num,
 target.Emp_Name = TRIM(source.Emp_Name),
 target.Emp_Address1 = TRIM(source.Emp_Address1),
 target.Emp_Address2 = TRIM(source.Emp_Address2),
 target.Emp_City = TRIM(source.Emp_City),
 target.Emp_State = TRIM(source.Emp_State),
 target.Emp_Zip = TRIM(source.Emp_Zip),
 target.Emp_Country = TRIM(source.Emp_Country),
 target.Emp_Contact = TRIM(source.Emp_Contact),
 target.Emp_Phone = TRIM(source.Emp_Phone),
 target.Emp_Phone_Ext = TRIM(source.Emp_Phone_Ext),
 target.Emp_Status = TRIM(source.Emp_Status),
 target.Ins_Name = TRIM(source.Ins_Name),
 target.Ins_Address1 = TRIM(source.Ins_Address1),
 target.Ins_Address2 = TRIM(source.Ins_Address2),
 target.Ins_City = TRIM(source.Ins_City),
 target.Ins_State = TRIM(source.Ins_State),
 target.Ins_Zip = TRIM(source.Ins_Zip),
 target.Ins_Country = TRIM(source.Ins_Country),
 target.Ins_Phone = TRIM(source.Ins_Phone),
 target.Ins_Phone_Ext = TRIM(source.Ins_Phone_Ext),
 target.Ins_SSN = TRIM(source.Ins_SSN),
 target.Ins_Sex = TRIM(source.Ins_Sex),
 target.Ins_Birthday = source.Ins_Birthday,
 target.Ins_Relationship = TRIM(source.Ins_Relationship),
 target.Deleted = TRIM(source.Deleted),
 target.Active = TRIM(source.Active),
 target.Last_Trans_Date = source.Last_Trans_Date,
 target.Crt_UserNum = source.Crt_UserNum,
 target.Crt_UserID = TRIM(source.Crt_UserID),
 target.Crt_DateTime = source.Crt_DateTime,
 target.Last_Upd_UserNum = source.Last_Upd_UserNum,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.PatBillingPK = TRIM(source.PatBillingPK),
 target.PatBillingPK_txt = TRIM(source.PatBillingPK_txt),
 target.LastUpdateServerTime = source.LastUpdateServerTime,
 target.Insurance_Type = TRIM(source.Insurance_Type),
 target.Emp_EmployerPK = TRIM(source.Emp_EmployerPK),
 target.Emp_EmployerPK_txt = TRIM(source.Emp_EmployerPK_txt),
 target.Prim_CompanyPK = TRIM(source.Prim_CompanyPK),
 target.Prim_CompanyPK_txt = TRIM(source.Prim_CompanyPK_txt),
 target.Prim_InsurancePK = TRIM(source.Prim_InsurancePK),
 target.Prim_InsurancePK_txt = TRIM(source.Prim_InsurancePK_txt),
 target.RegionKey = source.RegionKey,
 target.sysstarttime = source.sysstarttime,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Practice, Pat_Num, Payer_Num, Bill_To, Short_Name, CLASS, TYPE, Priority, Pricing, WC_Pricing, EPS_Pricing, PBC_Flag, Percentage, Assignment_Date, Assignment_Flag, Eft_Date, Exp_Date, Policy_Notes, Svc_Copay, Med_Copay, Member_ID, Group_ID, Auth_Num, Crg_Balance, Prim_Name, Prim_Address1, Prim_Address2, Prim_City, Prim_State, Prim_Zip, Prim_Country, Prim_Attn, Prim_Phone, Prim_Phone_Ext, Scnd_Name, Scnd_Address1, Scnd_Address2, Scnd_City, Scnd_State, Scnd_Zip, Scnd_Country, Scnd_Contact, Scnd_Phone, Scnd_Phone_Ext, Emp_Num, Emp_Name, Emp_Address1, Emp_Address2, Emp_City, Emp_State, Emp_Zip, Emp_Country, Emp_Contact, Emp_Phone, Emp_Phone_Ext, Emp_Status, Ins_Name, Ins_Address1, Ins_Address2, Ins_City, Ins_State, Ins_Zip, Ins_Country, Ins_Phone, Ins_Phone_Ext, Ins_SSN, Ins_Sex, Ins_Birthday, Ins_Relationship, Deleted, Active, Last_Trans_Date, Crt_UserNum, Crt_UserID, Crt_DateTime, Last_Upd_UserNum, Last_Upd_UserID, Last_Upd_DateTime, PatBillingPK, PatBillingPK_txt, LastUpdateServerTime, Insurance_Type, Emp_EmployerPK, Emp_EmployerPK_txt, Prim_CompanyPK, Prim_CompanyPK_txt, Prim_InsurancePK, Prim_InsurancePK_txt, RegionKey, sysstarttime, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Practice), source.Pat_Num, source.Payer_Num, TRIM(source.Bill_To), TRIM(source.Short_Name), TRIM(source.CLASS), TRIM(source.TYPE), source.Priority, TRIM(source.Pricing), TRIM(source.WC_Pricing), TRIM(source.EPS_Pricing), TRIM(source.PBC_Flag), source.Percentage, source.Assignment_Date, TRIM(source.Assignment_Flag), source.Eft_Date, source.Exp_Date, TRIM(source.Policy_Notes), source.Svc_Copay, source.Med_Copay, TRIM(source.Member_ID), TRIM(source.Group_ID), TRIM(source.Auth_Num), source.Crg_Balance, TRIM(source.Prim_Name), TRIM(source.Prim_Address1), TRIM(source.Prim_Address2), TRIM(source.Prim_City), TRIM(source.Prim_State), TRIM(source.Prim_Zip), TRIM(source.Prim_Country), TRIM(source.Prim_Attn), TRIM(source.Prim_Phone), TRIM(source.Prim_Phone_Ext), TRIM(source.Scnd_Name), TRIM(source.Scnd_Address1), TRIM(source.Scnd_Address2), TRIM(source.Scnd_City), TRIM(source.Scnd_State), TRIM(source.Scnd_Zip), TRIM(source.Scnd_Country), TRIM(source.Scnd_Contact), TRIM(source.Scnd_Phone), TRIM(source.Scnd_Phone_Ext), source.Emp_Num, TRIM(source.Emp_Name), TRIM(source.Emp_Address1), TRIM(source.Emp_Address2), TRIM(source.Emp_City), TRIM(source.Emp_State), TRIM(source.Emp_Zip), TRIM(source.Emp_Country), TRIM(source.Emp_Contact), TRIM(source.Emp_Phone), TRIM(source.Emp_Phone_Ext), TRIM(source.Emp_Status), TRIM(source.Ins_Name), TRIM(source.Ins_Address1), TRIM(source.Ins_Address2), TRIM(source.Ins_City), TRIM(source.Ins_State), TRIM(source.Ins_Zip), TRIM(source.Ins_Country), TRIM(source.Ins_Phone), TRIM(source.Ins_Phone_Ext), TRIM(source.Ins_SSN), TRIM(source.Ins_Sex), source.Ins_Birthday, TRIM(source.Ins_Relationship), TRIM(source.Deleted), TRIM(source.Active), source.Last_Trans_Date, source.Crt_UserNum, TRIM(source.Crt_UserID), source.Crt_DateTime, source.Last_Upd_UserNum, TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, TRIM(source.PatBillingPK), TRIM(source.PatBillingPK_txt), source.LastUpdateServerTime, TRIM(source.Insurance_Type), TRIM(source.Emp_EmployerPK), TRIM(source.Emp_EmployerPK_txt), TRIM(source.Prim_CompanyPK), TRIM(source.Prim_CompanyPK_txt), TRIM(source.Prim_InsurancePK), TRIM(source.Prim_InsurancePK_txt), source.RegionKey, source.sysstarttime, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PatBillingPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgPatBilling
      GROUP BY PatBillingPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgPatBilling');
ELSE
  COMMIT TRANSACTION;
END IF;
