CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refusersmeditechexpanse`
AS SELECT
  `ecw_refusersmeditechexpanse`.userkey,
  `ecw_refusersmeditechexpanse`.regionkey,
  `ecw_refusersmeditechexpanse`.username,
  `ecw_refusersmeditechexpanse`.userfirstname,
  `ecw_refusersmeditechexpanse`.userlastname,
  `ecw_refusersmeditechexpanse`.usermiddlename,
  `ecw_refusersmeditechexpanse`.userfullname,
  `ecw_refusersmeditechexpanse`.userprofileid,
  `ecw_refusersmeditechexpanse`.userproviderid,
  `ecw_refusersmeditechexpanse`.deleteflag,
  `ecw_refusersmeditechexpanse`.sourceprimarykeyvalue,
  `ecw_refusersmeditechexpanse`.sourcearecordlastupdated,
  `ecw_refusersmeditechexpanse`.dwlastupdatedatetime,
  `ecw_refusersmeditechexpanse`.sourcesystemcode,
  `ecw_refusersmeditechexpanse`.insertedby,
  `ecw_refusersmeditechexpanse`.inserteddtm,
  `ecw_refusersmeditechexpanse`.modifiedby,
  `ecw_refusersmeditechexpanse`.modifieddtm
  FROM
    edwpsc.`ecw_refusersmeditechexpanse`
;