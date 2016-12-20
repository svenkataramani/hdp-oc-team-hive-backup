SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

--create table hdp_oc_team.ShopNPSResp stored as textfile as
--select shopper_id, survey_name, response_collected_date
--from conductor.satmetrix_relationship_survey_results_nsm;

insert overwrite table hdp_oc_team.ShopNPSResp
select shopper_id, survey_name, response_collected_date
from conductor.satmetrix_relationship_survey_results_nsm;