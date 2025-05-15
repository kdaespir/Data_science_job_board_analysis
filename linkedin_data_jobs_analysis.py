import pandas as pd
import duckdb

data = pd.read_csv('clean_jobs.csv')

# Gives the number of null columns in each feature
# print(data.isnull().sum())

companies_breakdown = duckdb.sql("""
            select company, count(id) as counts
            from data
            group by company
            order by counts desc, company desc
               """)

companies_w_most_jobs = duckdb.sql('''
               with cte as (
               select *, 
               from companies_breakdown
               limit 4)

               select *
               from cte

               union 

               select 'Other' as company, sum(counts) as count
               from companies_breakdown 
               where company not in (select company from cte)

               order by company
               ''').df()
               

print(_)