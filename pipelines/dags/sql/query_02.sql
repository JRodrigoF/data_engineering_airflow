SELECT count(weekday) as total, weekday FROM fact_table_memes f
JOIN dim_dates dd on f.date_added_Id=dd.date_Id
join dim_status ds on f.status_Id=ds.status_Id
group by weekday
order by weekday asc
