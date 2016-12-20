use hdp_oc_team;

--create table hdp_oc_team.FreeTrialOrders stored as textfile as
--select distinct resource_id, shopper_id, order_id, row_id, pf_id, order_source, free_order_dt, cancel_date, purchasetypeid, expiration_date, paid_renewal_date
--from bisandbox.unifiedbillingsnapshot_trial;

insert overwrite table hdp_oc_team.FreeTrialOrders
select distinct resource_id, shopper_id, order_id, row_id, pf_id, order_source, free_order_dt, cancel_date, purchasetypeid, expiration_date, paid_renewal_date
from bisandbox.unifiedbillingsnapshot_trial;