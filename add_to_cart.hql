USE hdp_oc_team;

SET hive.support.concurrency=false;
SET hive.exec.parallel = true;
SET hive.groupby.orderby.position.alias=TRUE;

INSERT OVERWRITE TABLE hdp_oc_team.add_to_cart_stage
SELECT
DISTINCT
cart_date,
shopper_id,
LOWER(vguid) as vguid,
LOWER(domain_name) as domain_name,
LOWER(cart_tld_name) as cart_tld_name
FROM bisandbox.add_to_cart
WHERE cast(cart_date as date) = '${hiveconf:yesterday_date}'
    and vguid IS NOT NULL
    AND regexp_extract(shopper_id,'[a-zA-Z]+', 0) = ''-- ignore employee shoppers
;

