
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

MERGE INTO {{ params.param_psc_core_dataset_name }}.PV_StgPhysician AS target
USING {{ params.param_psc_stage_dataset_name }}.PV_StgPhysician AS source
ON target.PhysicianPK = source.PhysicianPK AND target.RegionKey = source.RegionKey
WHEN MATCHED THEN
  UPDATE SET
  target.Practice = TRIM(source.Practice),
 target.Phy_Num = source.Phy_Num,
 target.PiVoT_Provider_Num = TRIM(source.PiVoT_Provider_Num),
 target.Description = TRIM(source.Description),
 target.First_Name = TRIM(source.First_Name),
 target.Middle_Name = TRIM(source.Middle_Name),
 target.Last_Name = TRIM(source.Last_Name),
 target.Address1 = TRIM(source.Address1),
 target.Address2 = TRIM(source.Address2),
 target.City = TRIM(source.City),
 target.State = TRIM(source.State),
 target.Zip = TRIM(source.Zip),
 target.Home_Phone = TRIM(source.Home_Phone),
 target.Work_Phone = TRIM(source.Work_Phone),
 target.Pager = TRIM(source.Pager),
 target.Cell_Phone = TRIM(source.Cell_Phone),
 target.Active = TRIM(source.Active),
 target.Member_Flag = TRIM(source.Member_Flag),
 target.Ref_Phy_Flag = TRIM(source.Ref_Phy_Flag),
 target.UPIN = TRIM(source.UPIN),
 target.NPI = TRIM(source.NPI),
 target.Provider_Type = TRIM(source.Provider_Type),
 target.Fed_Tax_ID = TRIM(source.Fed_Tax_ID),
 target.Taxonomy_Code = TRIM(source.Taxonomy_Code),
 target.SSN = TRIM(source.SSN),
 target.License = TRIM(source.License),
 target.Credential = TRIM(source.Credential),
 target.DEA_Number = TRIM(source.DEA_Number),
 target.MC_Provider_Num = TRIM(source.MC_Provider_Num),
 target.PA_Payee_Num = TRIM(source.PA_Payee_Num),
 target.Bill_To = source.Bill_To,
 target.Last_Upd_UserID = TRIM(source.Last_Upd_UserID),
 target.Last_Upd_DateTime = source.Last_Upd_DateTime,
 target.PhysicianPK = TRIM(source.PhysicianPK),
 target.WC_Billing_Num = TRIM(source.WC_Billing_Num),
 target.LastUpdateServerTime = source.LastUpdateServerTime,
 target.Fax = TRIM(source.Fax),
 target.Email = TRIM(source.Email),
 target.OnScheduler = source.OnScheduler,
 target.RegionKey = source.RegionKey,
 target.InsertedDTM = source.InsertedDTM,
 target.ModifiedDTM = source.ModifiedDTM,
 target.DWLastUpdateDateTime = source.DWLastUpdateDateTime,
 target.SourcePhysicalDeleteFlag = source.SourcePhysicalDeleteFlag
WHEN NOT MATCHED THEN
  INSERT (Practice, Phy_Num, PiVoT_Provider_Num, Description, First_Name, Middle_Name, Last_Name, Address1, Address2, City, State, Zip, Home_Phone, Work_Phone, Pager, Cell_Phone, Active, Member_Flag, Ref_Phy_Flag, UPIN, NPI, Provider_Type, Fed_Tax_ID, Taxonomy_Code, SSN, License, Credential, DEA_Number, MC_Provider_Num, PA_Payee_Num, Bill_To, Last_Upd_UserID, Last_Upd_DateTime, PhysicianPK, WC_Billing_Num, LastUpdateServerTime, Fax, Email, OnScheduler, RegionKey, InsertedDTM, ModifiedDTM, DWLastUpdateDateTime, SourcePhysicalDeleteFlag)
  VALUES (TRIM(source.Practice), source.Phy_Num, TRIM(source.PiVoT_Provider_Num), TRIM(source.Description), TRIM(source.First_Name), TRIM(source.Middle_Name), TRIM(source.Last_Name), TRIM(source.Address1), TRIM(source.Address2), TRIM(source.City), TRIM(source.State), TRIM(source.Zip), TRIM(source.Home_Phone), TRIM(source.Work_Phone), TRIM(source.Pager), TRIM(source.Cell_Phone), TRIM(source.Active), TRIM(source.Member_Flag), TRIM(source.Ref_Phy_Flag), TRIM(source.UPIN), TRIM(source.NPI), TRIM(source.Provider_Type), TRIM(source.Fed_Tax_ID), TRIM(source.Taxonomy_Code), TRIM(source.SSN), TRIM(source.License), TRIM(source.Credential), TRIM(source.DEA_Number), TRIM(source.MC_Provider_Num), TRIM(source.PA_Payee_Num), source.Bill_To, TRIM(source.Last_Upd_UserID), source.Last_Upd_DateTime, TRIM(source.PhysicianPK), TRIM(source.WC_Billing_Num), source.LastUpdateServerTime, TRIM(source.Fax), TRIM(source.Email), source.OnScheduler, source.RegionKey, source.InsertedDTM, source.ModifiedDTM, source.DWLastUpdateDateTime, source.SourcePhysicalDeleteFlag);

SET DUP_COUNT = (
  SELECT COUNT(*)
  FROM (
      SELECT PhysicianPK, RegionKey
      FROM {{ params.param_psc_core_dataset_name }}.PV_StgPhysician
      GROUP BY PhysicianPK, RegionKey
      HAVING COUNT(*) > 1
  )
);

IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.PV_StgPhysician');
ELSE
  COMMIT TRANSACTION;
END IF;
