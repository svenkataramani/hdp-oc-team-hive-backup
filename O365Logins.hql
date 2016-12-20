use hdp_oc_team;

insert overwrite table hdp_oc_team.O365_logins
select d.gd_shopper_id as shopper_id, max(to_date(a.event_time)) as login_date
from dm_product.dm_o365_dcr_cauth_login_history a,
wopr.mailbox_snap b,
wopr.account_snap c,
wopr.shopper_snap d
where a.user = b.email_address
and b.account_id = c.id
and c.shopper_id = d.id
and to_date(a.event_time) >= date_sub(to_date(from_unixtime(unix_timestamp())), 14)
and regexp_extract(d.gd_shopper_id,'[a-zA-Z]+', 0) = ''
group by d.gd_shopper_id, a.event_id;