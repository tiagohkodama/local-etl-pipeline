-- Sample queries to explore ETL results
-- Recent rows
select * from etl.results order by created_at desc limit 50;

-- Totals by date
select date, sum(total_amount) as total from etl.results group by date order by date;
