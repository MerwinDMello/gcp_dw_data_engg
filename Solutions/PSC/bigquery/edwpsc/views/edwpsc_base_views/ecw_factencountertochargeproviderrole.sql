CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factencountertochargeproviderrole`
AS SELECT
  `ecw_factencountertochargeproviderrole`.encountertochargeproviderrole,
  `ecw_factencountertochargeproviderrole`.encountertochargekey,
  `ecw_factencountertochargeproviderrole`.providerrole,
  `ecw_factencountertochargeproviderrole`.providername,
  `ecw_factencountertochargeproviderrole`.facilitymnemonic,
  `ecw_factencountertochargeproviderrole`.providermnemonic,
  `ecw_factencountertochargeproviderrole`.providernpi,
  `ecw_factencountertochargeproviderrole`.providerdealicensenumber,
  `ecw_factencountertochargeproviderrole`.pscproviderflag,
  `ecw_factencountertochargeproviderrole`.assigneddtm,
  `ecw_factencountertochargeproviderrole`.dwlastupdatedatetime,
  `ecw_factencountertochargeproviderrole`.insertedby,
  `ecw_factencountertochargeproviderrole`.inserteddtm,
  `ecw_factencountertochargeproviderrole`.modifiedby,
  `ecw_factencountertochargeproviderrole`.modifieddtm
  FROM
    edwpsc.`ecw_factencountertochargeproviderrole`
;