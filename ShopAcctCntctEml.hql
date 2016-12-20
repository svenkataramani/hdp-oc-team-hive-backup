USE hdp_oc_team;

SET hive.support.concurrency=false;
SET hive.exec.parallel = true;
SET hive.groupby.orderby.position.alias=TRUE;


--set today=cast(to_date(from_unixtime(unix_timestamp())) as string);
--Select ${hiveconf:today};

--set today='2016-04-28';
--Select ${hiveconf:today};


With cm as
(
SELECT
shopperid,
customermap_id
FROM cust_contact.customermap_snap
WHERE shopperid rlike '^[0-9]+$' = TRUE
),


ce AS
(
select
customermap_id,
email_id,
SUBSTRING(sentdate,0,10) AS sentdate,
CONCAT(SUBSTRING(sentdate,0,19),'.000') AS sentdatetime,
privatelabel_id,
isccode,
emailsource_id,
emailsourcekey,
template_id,
message_id,
batch_id,
errorcode_id,
emailformat,
lang,
auditcreatedatetime,
tx_date
FROM cust_contact_txlog.contact_email
--WHERE tx_date = '2016-04-28'
WHERE tx_date = '${hiveconf:current_date}'
AND SUBSTRING(sentdate,0,10) = '${hiveconf:current_date}'
)




insert overwrite table hdp_oc_team.ShopAcctCntctEml
select distinct shopperid,
cm.customermap_id,
email_id,
sentdate,
sentdatetime,
privatelabel_id,
isccode,
emailsource_id,
emailsourcekey,
template_id,
message_id,
batch_id,
errorcode_id,
emailformat,
lang,
auditcreatedatetime
from ce JOIN cm
ON ce.customermap_id = cm.customermap_id
;

