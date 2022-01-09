SELECT count(safeness_category) as total, safeness_category from fact_table_memes f
join dim_safeness_gv ds on f.safeness_Id=ds.safeness_Id
group by safeness_category
