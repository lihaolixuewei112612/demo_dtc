资产大盘
```sbtshell
select n.`name`,n.zc_name,n.num from (select * from (select m.zc_name,m.parent_id as pd,count(*) as num from (select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a 
left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m GROUP BY m.zc_name) x left join asset_category y on x.pd = y.id) n group by n.`name`,n.zc_name
```
