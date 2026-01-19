CREATE OR REPLACE VIEW edwpsc_views.`artiva_stghcdttypes`
AS SELECT
  `artiva_stghcdttypes`.hcdtdesc,
  `artiva_stghcdttypes`.hcdtid
  FROM
    edwpsc_base_views.`artiva_stghcdttypes`
;