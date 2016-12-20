SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

--create table hdp_oc_team.ShopNPSURL stored as textfile as
--select shopperid, url
--from satmetrix_nps.shopperurl;

insert overwrite table hdp_oc_team.ShopNPSURL
SELECT a.shopper_id as shopperid, a.survey_link as URL
FROM conductor.satmetrix_shopper_url_mappings_nsm a
INNER JOIN
(SELECT max(start_dt) as md
FROM conductor.satmetrix_shopper_url_mappings_nsm
WHERE to_date(start_dt) >= date_sub(FROM_UNIXTIME(UNIX_TIMESTAMP()), 5)) b ON to_date(a.start_dt) = to_date(b.md);