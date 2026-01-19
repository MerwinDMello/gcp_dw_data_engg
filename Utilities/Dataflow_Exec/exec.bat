python -m test_df ^
--region us-east4 ^
--output hca-hin-dev-cur-parallon:edwpbs.test1 ^
--runner DataflowRunner ^
--project hca-hin-dev-proc-parallon ^
--temp_location gs://eim-parallon-cs-datamig-dev-0002/temp ^
--staging_location gs://eim-parallon-cs-datamig-dev-0002/dfstage