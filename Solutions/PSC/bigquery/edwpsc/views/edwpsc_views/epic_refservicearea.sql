CREATE OR REPLACE VIEW edwpsc_views.`epic_refservicearea`
AS SELECT
  `epic_refservicearea`.serviceareakey,
  `epic_refservicearea`.serviceareaname,
  `epic_refservicearea`.serviceareaabbr,
  `epic_refservicearea`.serviceareatype,
  `epic_refservicearea`.serviceareagroup,
  `epic_refservicearea`.serviceareaglprefix,
  `epic_refservicearea`.servareaid,
  `epic_refservicearea`.regionkey,
  `epic_refservicearea`.sourceaprimarykey,
  `epic_refservicearea`.dwlastupdatedatetime,
  `epic_refservicearea`.sourcesystemcode,
  `epic_refservicearea`.insertedby,
  `epic_refservicearea`.inserteddtm,
  `epic_refservicearea`.modifiedby,
  `epic_refservicearea`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refservicearea`
;