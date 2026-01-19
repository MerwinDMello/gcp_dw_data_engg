
TRUNCATE TABLE {{ params.param_psc_core_dataset_name }}.ECW_JuncArtivaPPItoClaim ;

INSERT INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncArtivaPPItoClaim
(artivappitoclaimkey, claimkey, claimnumber, regionkey, coid, practicefederaltaxid, cpid, providerid, providernpi, facilityid, payerfinancialclass, ppikey, ppieffectivedate, holdruleid, dwlastupdatedatetime, sourcesystemcode, insertedby, inserteddtm, modifiedby, modifieddtm, payer1practicefederaltaxid, payer1facilityid, payer1cpid, payer1ppikey, payer1ppieffectivedate, payer1financialclass, arclaimflag)
SELECT source.artivappitoclaimkey, source.claimkey, source.claimnumber, source.regionkey, TRIM(source.coid), TRIM(source.practicefederaltaxid), TRIM(source.cpid), TRIM(source.providerid), TRIM(source.providernpi), TRIM(source.facilityid), TRIM(source.payerfinancialclass), TRIM(source.ppikey), source.ppieffectivedate, source.holdruleid, source.dwlastupdatedatetime, TRIM(source.sourcesystemcode), TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, TRIM(source.payer1practicefederaltaxid), TRIM(source.payer1facilityid), TRIM(source.payer1cpid), TRIM(source.payer1ppikey), source.payer1ppieffectivedate, TRIM(source.payer1financialclass), source.arclaimflag
FROM {{ params.param_psc_stage_dataset_name }}.ECW_JuncArtivaPPItoClaim as source;


-- DECLARE DUP_COUNT INT64;

-- BEGIN TRANSACTION;

-- MERGE INTO {{ params.param_psc_core_dataset_name }}.ECW_JuncArtivaPPItoClaim AS target
-- USING {{ params.param_psc_stage_dataset_name }}.ECW_JuncArtivaPPItoClaim AS source
-- ON target.ArtivaPPItoClaimKey = source.ArtivaPPItoClaimKey
-- WHEN MATCHED THEN
--   UPDATE SET
--   target.artivappitoclaimkey = source.artivappitoclaimkey,
--  target.claimkey = source.claimkey,
--  target.claimnumber = source.claimnumber,
--  target.regionkey = source.regionkey,
--  target.coid = TRIM(source.coid),
--  target.practicefederaltaxid = TRIM(source.practicefederaltaxid),
--  target.cpid = TRIM(source.cpid),
--  target.providerid = TRIM(source.providerid),
--  target.providernpi = TRIM(source.providernpi),
--  target.facilityid = TRIM(source.facilityid),
--  target.payerfinancialclass = TRIM(source.payerfinancialclass),
--  target.ppikey = TRIM(source.ppikey),
--  target.ppieffectivedate = source.ppieffectivedate,
--  target.holdruleid = source.holdruleid,
--  target.dwlastupdatedatetime = source.dwlastupdatedatetime,
--  target.sourcesystemcode = TRIM(source.sourcesystemcode),
--  target.insertedby = TRIM(source.insertedby),
--  target.inserteddtm = source.inserteddtm,
--  target.modifiedby = TRIM(source.modifiedby),
--  target.modifieddtm = source.modifieddtm,
--  target.payer1practicefederaltaxid = TRIM(source.payer1practicefederaltaxid),
--  target.payer1facilityid = TRIM(source.payer1facilityid),
--  target.payer1cpid = TRIM(source.payer1cpid),
--  target.payer1ppikey = TRIM(source.payer1ppikey),
--  target.payer1ppieffectivedate = source.payer1ppieffectivedate,
--  target.payer1financialclass = TRIM(source.payer1financialclass),
--  target.arclaimflag = source.arclaimflag
-- WHEN NOT MATCHED THEN
--   INSERT (artivappitoclaimkey, claimkey, claimnumber, regionkey, coid, practicefederaltaxid, cpid, providerid, providernpi, facilityid, payerfinancialclass, ppikey, ppieffectivedate, holdruleid, dwlastupdatedatetime, sourcesystemcode, insertedby, inserteddtm, modifiedby, modifieddtm, payer1practicefederaltaxid, payer1facilityid, payer1cpid, payer1ppikey, payer1ppieffectivedate, payer1financialclass, arclaimflag)
--   VALUES (source.artivappitoclaimkey, source.claimkey, source.claimnumber, source.regionkey, TRIM(source.coid), TRIM(source.practicefederaltaxid), TRIM(source.cpid), TRIM(source.providerid), TRIM(source.providernpi), TRIM(source.facilityid), TRIM(source.payerfinancialclass), TRIM(source.ppikey), source.ppieffectivedate, source.holdruleid, source.dwlastupdatedatetime, TRIM(source.sourcesystemcode), TRIM(source.insertedby), source.inserteddtm, TRIM(source.modifiedby), source.modifieddtm, TRIM(source.payer1practicefederaltaxid), TRIM(source.payer1facilityid), TRIM(source.payer1cpid), TRIM(source.payer1ppikey), source.payer1ppieffectivedate, TRIM(source.payer1financialclass), source.arclaimflag);

-- SET DUP_COUNT = (
--   SELECT COUNT(*)
--   FROM (
--       SELECT ArtivaPPItoClaimKey
--       FROM {{ params.param_psc_core_dataset_name }}.ECW_JuncArtivaPPItoClaim
--       GROUP BY ArtivaPPItoClaimKey
--       HAVING COUNT(*) > 1
--   )
-- );

-- IF DUP_COUNT <> 0 THEN
--   ROLLBACK TRANSACTION;
--   RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_psc_core_dataset_name }}.ECW_JuncArtivaPPItoClaim');
-- ELSE
--   COMMIT TRANSACTION;
-- END IF;
