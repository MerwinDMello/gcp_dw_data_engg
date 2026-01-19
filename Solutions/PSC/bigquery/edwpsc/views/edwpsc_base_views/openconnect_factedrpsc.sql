CREATE OR REPLACE VIEW edwpsc_base_views.`openconnect_factedrpsc`
AS SELECT
  `openconnect_factedrpsc`.openconnectedrkey,
  `openconnect_factedrpsc`.sendingapplication,
  `openconnect_factedrpsc`.messageid,
  `openconnect_factedrpsc`.actionid,
  `openconnect_factedrpsc`.actionname,
  `openconnect_factedrpsc`.errorcategory,
  `openconnect_factedrpsc`.crosswalkerror,
  `openconnect_factedrpsc`.crosswalkerrorrollup,
  `openconnect_factedrpsc`.dateerrorreceived,
  `openconnect_factedrpsc`.errorid,
  `openconnect_factedrpsc`.errormessage,
  `openconnect_factedrpsc`.artivaloaddate,
  `openconnect_factedrpsc`.notes,
  `openconnect_factedrpsc`.originalartivaloaddate,
  `openconnect_factedrpsc`.routestepstatusreasonid,
  `openconnect_factedrpsc`.routestepstatusreason,
  `openconnect_factedrpsc`.routestepid,
  `openconnect_factedrpsc`.routestepname,
  `openconnect_factedrpsc`.psoceocid,
  `openconnect_factedrpsc`.insertedby,
  `openconnect_factedrpsc`.inserteddtm,
  `openconnect_factedrpsc`.dwlastupdatedatetime
  FROM
    edwpsc.`openconnect_factedrpsc`
;