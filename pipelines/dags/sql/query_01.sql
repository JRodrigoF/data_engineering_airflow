SELECT count(year) as total, year, details_status FROM fact_table_memes f
JOIN dim_dates dd on f.date_added_Id=dd.date_Id
join dim_status ds on f.status_Id=ds.status_Id
group by year, details_status
order by year asc
