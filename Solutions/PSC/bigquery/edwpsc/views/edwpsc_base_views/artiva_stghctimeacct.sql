CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stghctimeacct`
AS SELECT
  `artiva_stghctimeacct`.hctimekey,
  `artiva_stghctimeacct`.hctimeid,
  `artiva_stghctimeacct`.hctimeencntrid,
  `artiva_stghctimeacct`.hctimeuser,
  `artiva_stghctimeacct`.hctimepyr,
  `artiva_stghctimeacct`.hctimedate,
  `artiva_stghctimeacct`.hctimetime,
  `artiva_stghctimeacct`.hctimestime,
  `artiva_stghctimeacct`.hctimeetime,
  `artiva_stghctimeacct`.hctimettime,
  `artiva_stghctimeacct`.userid,
  `artiva_stghctimeacct`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stghctimeacct`
;