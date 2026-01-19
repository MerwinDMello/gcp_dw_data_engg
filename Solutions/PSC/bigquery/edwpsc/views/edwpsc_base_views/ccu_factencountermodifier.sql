CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_factencountermodifier`
AS SELECT
  `ccu_factencountermodifier`.id,
  `ccu_factencountermodifier`.encounterid,
  `ccu_factencountermodifier`.invoiceid,
  `ccu_factencountermodifier`.patientid,
  `ccu_factencountermodifier`.displayindex,
  `ccu_factencountermodifier`.itemid,
  `ccu_factencountermodifier`.meddesc,
  `ccu_factencountermodifier`.date,
  `ccu_factencountermodifier`.deleteflag,
  `ccu_factencountermodifier`.encmod1,
  `ccu_factencountermodifier`.encmod2,
  `ccu_factencountermodifier`.encmod3,
  `ccu_factencountermodifier`.encmod4,
  `ccu_factencountermodifier`.created,
  `ccu_factencountermodifier`.modified,
  `ccu_factencountermodifier`.dwlastupdatedatetime,
  `ccu_factencountermodifier`.regionkey,
  `ccu_factencountermodifier`.ccuencountermodifierkey
  FROM
    edwpsc.`ccu_factencountermodifier`
;