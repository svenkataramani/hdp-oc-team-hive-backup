SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

Insert Overwrite Table hdp_oc_team.campaign
SELECT DISTINCT 
  m.Shopper_id AS Shopper_id
, c.campaign_id AS campaign_id
, c.campaign_name AS campaign_name
, a.audience_id AS audience_id
, a.audience_description AS audience_description
, t.treatment_id AS treatment_id
, t.treatment_name AS treatment_name
, m.inclusion_date AS inclusion_date
FROM dbmarketing.cge_campaigns c
LEFT JOIN dbmarketing.cge_campaign_audience a on c.campaign_id=a.campaign_id
LEFT JOIN dbmarketing.cge_campaign_test t on a.audience_id=t.audience_id AND a.campaign_id=t.campaign_id
LEFT JOIN dbmarketing.cge_campaign_contact_map m on m.campaign_id=t.campaign_id AND m.treatment_id=t.treatment_id AND m.audience_id=t.audience_id;