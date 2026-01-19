
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgInsurance AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgInsurance AS source
ON target.InsurancePK = source.InsurancePK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Short_Name = TRIM(source.Short_Name),
 target.CLASS = TRIM(source.CLASS),
 target.TYPE = TRIM(source.TYPE),
 target.Pricing = TRIM(source.Pricing),
 target.Bill_To = TRIM(source.Bill_To),
 target.EMC_PID = TRIM(source.EMC_PID),
 target.Paper_PID = TRIM(source.Paper_PID),
 target.Policy_Text = TRIM(source.Policy_Text),
 target.Svc_Copay = source.Svc_Copay,
 target.Med_Copay = source.Med_Copay,
 target.PBC_Flag = TRIM(source.PBC_Flag),
 target.Crr_Name = TRIM(source.Crr_Name),
 target.Crr_Address1 = TRIM(source.Crr_Address1),
 target.Crr_Address2 = TRIM(source.Crr_Address2),
 target.Crr_City = TRIM(source.Crr_City),
 target.Crr_State = TRIM(source.Crr_State),
 target.Crr_Zip = TRIM(source.Crr_Zip),
 target.Crr_Country = TRIM(source.Crr_Country),
 target.Crr_Attn = TRIM(source.Crr_Attn),
 target.Crr_Contact = TRIM(source.Crr_Contact),
 target.Crr_Phone = TRIM(source.Crr_Phone),
 target.Crr_Phone_Ext = TRIM(source.Crr_Phone_Ext),
 target.Crr_Fax = TRIM(source.Crr_Fax),
 target.Grp_Name = TRIM(source.Grp_Name),
 target.Grp_Address1 = TRIM(source.Grp_Address1),
 target.Grp_Address2 = TRIM(source.Grp_Address2),
 target.Grp_City = TRIM(source.Grp_City),
 target.Grp_State = TRIM(source.Grp_State),
 target.Grp_Zip = TRIM(source.Grp_Zip),
 target.Grp_Country = TRIM(source.Grp_Country),
 target.Grp_Attn = TRIM(source.Grp_Attn),
 target.Grp_Contact = TRIM(source.Grp_Contact),
 target.Grp_Phone = TRIM(source.Grp_Phone),
 target.Grp_Phone_Ext = TRIM(source.Grp_Phone_Ext),
 target.Grp_Fax = TRIM(source.Grp_Fax),
 target.InsuranceId = source.InsuranceId,
 target.InsurancePK = TRIM(source.InsurancePK),
 target.RegionKey = source.RegionKey,
 target.UseBillingNpiForRte = source.UseBillingNpiForRte,
 target.Active = TRIM(source.Active),
 target.InsurancePK_txt = TRIM(source.InsurancePK_txt),
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Practice, Short_Name, CLASS, TYPE, Pricing, Bill_To, EMC_PID, Paper_PID, Policy_Text, Svc_Copay, Med_Copay, PBC_Flag, Crr_Name, Crr_Address1, Crr_Address2, Crr_City, Crr_State, Crr_Zip, Crr_Country, Crr_Attn, Crr_Contact, Crr_Phone, Crr_Phone_Ext, Crr_Fax, Grp_Name, Grp_Address1, Grp_Address2, Grp_City, Grp_State, Grp_Zip, Grp_Country, Grp_Attn, Grp_Contact, Grp_Phone, Grp_Phone_Ext, Grp_Fax, InsuranceId, InsurancePK, RegionKey, UseBillingNpiForRte, Active, InsurancePK_txt, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Practice), TRIM(source.Short_Name), TRIM(source.CLASS), TRIM(source.TYPE), TRIM(source.Pricing), TRIM(source.Bill_To), TRIM(source.EMC_PID), TRIM(source.Paper_PID), TRIM(source.Policy_Text), source.Svc_Copay, source.Med_Copay, TRIM(source.PBC_Flag), TRIM(source.Crr_Name), TRIM(source.Crr_Address1), TRIM(source.Crr_Address2), TRIM(source.Crr_City), TRIM(source.Crr_State), TRIM(source.Crr_Zip), TRIM(source.Crr_Country), TRIM(source.Crr_Attn), TRIM(source.Crr_Contact), TRIM(source.Crr_Phone), TRIM(source.Crr_Phone_Ext), TRIM(source.Crr_Fax), TRIM(source.Grp_Name), TRIM(source.Grp_Address1), TRIM(source.Grp_Address2), TRIM(source.Grp_City), TRIM(source.Grp_State), TRIM(source.Grp_Zip), TRIM(source.Grp_Country), TRIM(source.Grp_Attn), TRIM(source.Grp_Contact), TRIM(source.Grp_Phone), TRIM(source.Grp_Phone_Ext), TRIM(source.Grp_Fax), source.InsuranceId, TRIM(source.InsurancePK), source.RegionKey, source.UseBillingNpiForRte, TRIM(source.Active), TRIM(source.InsurancePK_txt), source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT InsurancePK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgInsurance
      GROUP BY InsurancePK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgInsurance');
ELSE
  COMMIT TRANSACTION;
END IF;
