
INSERT INTO {{ params.param_psc_core_dataset_name }}.Artiva_InventorySnapShotDaily
(snapshotdate, snapshotdatetime, sourcesytem, hcpatientaccountingnumber, ecwclaimkey, ecwclaimnumber, ecwregionkey, pvclaimkey, pvclaimnumber, pvregionkey, hcenid, hcacid, hcbalance, liabilitybalance, primaryfinancialclass, liabilityfinancialclass, hclastloaddate, hclastworkeddate, psaccountcomplete, hccurrentpayerid, lastpool, hcsequencenumber, pspooldept, pspooltype, coid, rownum, hcmedicalrecordnumber, hcdateofservice, psecwclaimstatus, psacprevpool, psaclpoolstartdte, hcfinalbilldate, hcoriginalbalance, hctotaladjustments, hctotalpayments, hctotalcharges, hcstatuscode, hcdenialcode, hcdenialdate, hchostbalance, hcpayername, hcactioncode, hcresultcode, hcworkdate, hcvendorid, psmiscvalue, hcacpriphase, hcacpristatus, hcaccountphase, hcclaimsubmissiondate, last_action_code_time, last_action_code_user_id, last_action_date, pschecknumber, pvpracticename, hcfadesc, voidflag, deleteflag, arclaimflag, liabilityiplankey, liabilityownertype, primaryiplankey, waitdate, totalpaymentamt, totalinsurancepaymentamt, pspyclmtype, pspygrprptname, last_action_code_time2, last_action_code_user_id2, last_action_date2, tieramt, psentierthrshld, pspoolconctrlnum, dwlastupdatedatetime)
SELECT source.snapshotdate, source.snapshotdatetime, TRIM(source.sourcesytem), TRIM(source.hcpatientaccountingnumber), source.ecwclaimkey, source.ecwclaimnumber, source.ecwregionkey, source.pvclaimkey, source.pvclaimnumber, source.pvregionkey, TRIM(source.hcenid), TRIM(source.hcacid), source.hcbalance, source.liabilitybalance, TRIM(source.primaryfinancialclass), TRIM(source.liabilityfinancialclass), TRIM(source.hclastloaddate), TRIM(source.hclastworkeddate), source.psaccountcomplete, TRIM(source.hccurrentpayerid), TRIM(source.lastpool), source.hcsequencenumber, TRIM(source.pspooldept), TRIM(source.pspooltype), TRIM(source.coid), source.rownum, TRIM(source.hcmedicalrecordnumber), TRIM(source.hcdateofservice), TRIM(source.psecwclaimstatus), TRIM(source.psacprevpool), source.psaclpoolstartdte, TRIM(source.hcfinalbilldate), source.hcoriginalbalance, source.hctotaladjustments, source.hctotalpayments, source.hctotalcharges, TRIM(source.hcstatuscode), TRIM(source.hcdenialcode), TRIM(source.hcdenialdate), source.hchostbalance, TRIM(source.hcpayername), TRIM(source.hcactioncode), TRIM(source.hcresultcode), TRIM(source.hcworkdate), TRIM(source.hcvendorid), TRIM(source.psmiscvalue), TRIM(source.hcacpriphase), TRIM(source.hcacpristatus), TRIM(source.hcaccountphase), TRIM(source.hcclaimsubmissiondate), source.last_action_code_time, TRIM(source.last_action_code_user_id), source.last_action_date, TRIM(source.pschecknumber), TRIM(source.pvpracticename), TRIM(source.hcfadesc), source.voidflag, source.deleteflag, source.arclaimflag, source.liabilityiplankey, source.liabilityownertype, source.primaryiplankey, TRIM(source.waitdate), source.totalpaymentamt, source.totalinsurancepaymentamt, TRIM(source.pspyclmtype), TRIM(source.pspygrprptname), source.last_action_code_time2, TRIM(source.last_action_code_user_id2), source.last_action_date2, source.tieramt, TRIM(source.psentierthrshld), TRIM(source.pspoolconctrlnum), source.dwlastupdatedatetime
FROM {{ params.param_psc_stage_dataset_name }}.Artiva_InventorySnapShotDaily as source;


-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.Artiva_InventorySnapShotDaily AS target
-- USING {{ params.param_psc_stage_dataset_name }}.Artiva_InventorySnapShotDaily AS source
-- ON target.SnapShotDate = source.SnapShotDate
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.snapshotdate = source.snapshotdate,
--  target.snapshotdatetime = source.snapshotdatetime,
--  target.sourcesytem = TRIM(source.sourcesytem),
--  target.hcpatientaccountingnumber = TRIM(source.hcpatientaccountingnumber),
--  target.ecwclaimkey = source.ecwclaimkey,
--  target.ecwclaimnumber = source.ecwclaimnumber,
--  target.ecwregionkey = source.ecwregionkey,
--  target.pvclaimkey = source.pvclaimkey,
--  target.pvclaimnumber = source.pvclaimnumber,
--  target.pvregionkey = source.pvregionkey,
--  target.hcenid = TRIM(source.hcenid),
--  target.hcacid = TRIM(source.hcacid),
--  target.hcbalance = source.hcbalance,
--  target.liabilitybalance = source.liabilitybalance,
--  target.primaryfinancialclass = TRIM(source.primaryfinancialclass),
--  target.liabilityfinancialclass = TRIM(source.liabilityfinancialclass),
--  target.hclastloaddate = TRIM(source.hclastloaddate),
--  target.hclastworkeddate = TRIM(source.hclastworkeddate),
--  target.psaccountcomplete = source.psaccountcomplete,
--  target.hccurrentpayerid = TRIM(source.hccurrentpayerid),
--  target.lastpool = TRIM(source.lastpool),
--  target.hcsequencenumber = source.hcsequencenumber,
--  target.pspooldept = TRIM(source.pspooldept),
--  target.pspooltype = TRIM(source.pspooltype),
--  target.coid = TRIM(source.coid),
--  target.rownum = source.rownum,
--  target.hcmedicalrecordnumber = TRIM(source.hcmedicalrecordnumber),
--  target.hcdateofservice = TRIM(source.hcdateofservice),
--  target.psecwclaimstatus = TRIM(source.psecwclaimstatus),
--  target.psacprevpool = TRIM(source.psacprevpool),
--  target.psaclpoolstartdte = source.psaclpoolstartdte,
--  target.hcfinalbilldate = TRIM(source.hcfinalbilldate),
--  target.hcoriginalbalance = source.hcoriginalbalance,
--  target.hctotaladjustments = source.hctotaladjustments,
--  target.hctotalpayments = source.hctotalpayments,
--  target.hctotalcharges = source.hctotalcharges,
--  target.hcstatuscode = TRIM(source.hcstatuscode),
--  target.hcdenialcode = TRIM(source.hcdenialcode),
--  target.hcdenialdate = TRIM(source.hcdenialdate),
--  target.hchostbalance = source.hchostbalance,
--  target.hcpayername = TRIM(source.hcpayername),
--  target.hcactioncode = TRIM(source.hcactioncode),
--  target.hcresultcode = TRIM(source.hcresultcode),
--  target.hcworkdate = TRIM(source.hcworkdate),
--  target.hcvendorid = TRIM(source.hcvendorid),
--  target.psmiscvalue = TRIM(source.psmiscvalue),
--  target.hcacpriphase = TRIM(source.hcacpriphase),
--  target.hcacpristatus = TRIM(source.hcacpristatus),
--  target.hcaccountphase = TRIM(source.hcaccountphase),
--  target.hcclaimsubmissiondate = TRIM(source.hcclaimsubmissiondate),
--  target.last_action_code_time = source.last_action_code_time,
--  target.last_action_code_user_id = TRIM(source.last_action_code_user_id),
--  target.last_action_date = source.last_action_date,
--  target.pschecknumber = TRIM(source.pschecknumber),
--  target.pvpracticename = TRIM(source.pvpracticename),
--  target.hcfadesc = TRIM(source.hcfadesc),
--  target.voidflag = source.voidflag,
--  target.deleteflag = source.deleteflag,
--  target.arclaimflag = source.arclaimflag,
--  target.liabilityiplankey = source.liabilityiplankey,
--  target.liabilityownertype = source.liabilityownertype,
--  target.primaryiplankey = source.primaryiplankey,
--  target.waitdate = TRIM(source.waitdate),
--  target.totalpaymentamt = source.totalpaymentamt,
--  target.totalinsurancepaymentamt = source.totalinsurancepaymentamt,
--  target.pspyclmtype = TRIM(source.pspyclmtype),
--  target.pspygrprptname = TRIM(source.pspygrprptname),
--  target.last_action_code_time2 = source.last_action_code_time2,
--  target.last_action_code_user_id2 = TRIM(source.last_action_code_user_id2),
--  target.last_action_date2 = source.last_action_date2,
--  target.tieramt = source.tieramt,
--  target.psentierthrshld = TRIM(source.psentierthrshld),
--  target.pspoolconctrlnum = TRIM(source.pspoolconctrlnum),
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime
-- WHEN NOT MATCHED THEN
--   INSERT (snapshotdate, snapshotdatetime, sourcesytem, hcpatientaccountingnumber, ecwclaimkey, ecwclaimnumber, ecwregionkey, pvclaimkey, pvclaimnumber, pvregionkey, hcenid, hcacid, hcbalance, liabilitybalance, primaryfinancialclass, liabilityfinancialclass, hclastloaddate, hclastworkeddate, psaccountcomplete, hccurrentpayerid, lastpool, hcsequencenumber, pspooldept, pspooltype, coid, rownum, hcmedicalrecordnumber, hcdateofservice, psecwclaimstatus, psacprevpool, psaclpoolstartdte, hcfinalbilldate, hcoriginalbalance, hctotaladjustments, hctotalpayments, hctotalcharges, hcstatuscode, hcdenialcode, hcdenialdate, hchostbalance, hcpayername, hcactioncode, hcresultcode, hcworkdate, hcvendorid, psmiscvalue, hcacpriphase, hcacpristatus, hcaccountphase, hcclaimsubmissiondate, last_action_code_time, last_action_code_user_id, last_action_date, pschecknumber, pvpracticename, hcfadesc, voidflag, deleteflag, arclaimflag, liabilityiplankey, liabilityownertype, primaryiplankey, waitdate, totalpaymentamt, totalinsurancepaymentamt, pspyclmtype, pspygrprptname, last_action_code_time2, last_action_code_user_id2, last_action_date2, tieramt, psentierthrshld, pspoolconctrlnum, dwlastupdatedatetime)
--   VALUES (source.snapshotdate, source.snapshotdatetime, TRIM(source.sourcesytem), TRIM(source.hcpatientaccountingnumber), source.ecwclaimkey, source.ecwclaimnumber, source.ecwregionkey, source.pvclaimkey, source.pvclaimnumber, source.pvregionkey, TRIM(source.hcenid), TRIM(source.hcacid), source.hcbalance, source.liabilitybalance, TRIM(source.primaryfinancialclass), TRIM(source.liabilityfinancialclass), TRIM(source.hclastloaddate), TRIM(source.hclastworkeddate), source.psaccountcomplete, TRIM(source.hccurrentpayerid), TRIM(source.lastpool), source.hcsequencenumber, TRIM(source.pspooldept), TRIM(source.pspooltype), TRIM(source.coid), source.rownum, TRIM(source.hcmedicalrecordnumber), TRIM(source.hcdateofservice), TRIM(source.psecwclaimstatus), TRIM(source.psacprevpool), source.psaclpoolstartdte, TRIM(source.hcfinalbilldate), source.hcoriginalbalance, source.hctotaladjustments, source.hctotalpayments, source.hctotalcharges, TRIM(source.hcstatuscode), TRIM(source.hcdenialcode), TRIM(source.hcdenialdate), source.hchostbalance, TRIM(source.hcpayername), TRIM(source.hcactioncode), TRIM(source.hcresultcode), TRIM(source.hcworkdate), TRIM(source.hcvendorid), TRIM(source.psmiscvalue), TRIM(source.hcacpriphase), TRIM(source.hcacpristatus), TRIM(source.hcaccountphase), TRIM(source.hcclaimsubmissiondate), source.last_action_code_time, TRIM(source.last_action_code_user_id), source.last_action_date, TRIM(source.pschecknumber), TRIM(source.pvpracticename), TRIM(source.hcfadesc), source.voidflag, source.deleteflag, source.arclaimflag, source.liabilityiplankey, source.liabilityownertype, source.primaryiplankey, TRIM(source.waitdate), source.totalpaymentamt, source.totalinsurancepaymentamt, TRIM(source.pspyclmtype), TRIM(source.pspygrprptname), source.last_action_code_time2, TRIM(source.last_action_code_user_id2), source.last_action_date2, source.tieramt, TRIM(source.psentierthrshld), TRIM(source.pspoolconctrlnum), source.dwlastupdatedatetime);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT SnapShotDate
--       FROM {{ params.param_psc_core_dataset_name }}.Artiva_InventorySnapShotDaily
--       GROUP BY SnapShotDate
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.Artiva_InventorySnapShotDaily');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
