CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncencountertochargefirstlastkeymeasure`
AS SELECT
  `ecw_juncencountertochargefirstlastkeymeasure`.encountertochargekey,
  `ecw_juncencountertochargefirstlastkeymeasure`.lastsourcesystem,
  `ecw_juncencountertochargefirstlastkeymeasure`.lastsourcesystemdtm,
  `ecw_juncencountertochargefirstlastkeymeasure`.lastregionid,
  `ecw_juncencountertochargefirstlastkeymeasure`.laststatus,
  `ecw_juncencountertochargefirstlastkeymeasure`.laststatusdtm,
  `ecw_juncencountertochargefirstlastkeymeasure`.lastowner,
  `ecw_juncencountertochargefirstlastkeymeasure`.claimcount,
  `ecw_juncencountertochargefirstlastkeymeasure`.encountercptcount,
  `ecw_juncencountertochargefirstlastkeymeasure`.claimcptcount,
  `ecw_juncencountertochargefirstlastkeymeasure`.chargequantity,
  `ecw_juncencountertochargefirstlastkeymeasure`.pscproviderflag,
  `ecw_juncencountertochargefirstlastkeymeasure`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncencountertochargefirstlastkeymeasure`
;