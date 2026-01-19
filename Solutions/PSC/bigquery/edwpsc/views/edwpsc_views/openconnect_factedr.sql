CREATE OR REPLACE VIEW edwpsc_views.`openconnect_factedr`
AS SELECT
  `openconnect_factedr`.openconnectedrkey,
  `openconnect_factedr`.messageid,
  `openconnect_factedr`.actionid,
  `openconnect_factedr`.actionname,
  `openconnect_factedr`.errorcategory,
  `openconnect_factedr`.crosswalkerror,
  `openconnect_factedr`.crosswalkerrorrollup,
  `openconnect_factedr`.dateerrorreceived,
  `openconnect_factedr`.errorid,
  `openconnect_factedr`.errormessage,
  `openconnect_factedr`.artivaloaddate,
  `openconnect_factedr`.notes,
  `openconnect_factedr`.originalartivaloaddate,
  `openconnect_factedr`.routestepstatusreasonid,
  `openconnect_factedr`.routestepstatusreason,
  `openconnect_factedr`.routestepid,
  `openconnect_factedr`.routestepname,
  `openconnect_factedr`.psoceocid,
  `openconnect_factedr`.insertedby,
  `openconnect_factedr`.inserteddtm,
  `openconnect_factedr`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`openconnect_factedr`
;