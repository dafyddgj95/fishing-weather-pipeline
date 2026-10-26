select lake, local_date, count(*) as row_count
from {{ ref('fct_daily_summary') }}
group by lake, local_date
having count(*) > 1
