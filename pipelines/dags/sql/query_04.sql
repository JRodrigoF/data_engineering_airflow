SELECT count(details_origin) as total, details_origin, safeness_category from fact_table_memes f
join dim_origins on f.origin_Id=dim_origins.origin_Id
join dim_safeness_gv ds on f.safeness_Id=ds.safeness_Id
group by safeness_category, details_origin
having ds.safeness_category = %(category)s
order by total desc
limit 10
