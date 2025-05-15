import pandas as pd
import duckdb
import re

data = pd.read_csv('clean_jobs.csv')

# Gives the number of null columns in each feature
# print(data.isnull().sum())

def jobs_by_company(num_companies=4):
   companies_breakdown = duckdb.sql("""
               select company, count(id) as counts
               from data
               group by company
               order by counts desc, company desc
                  """)

   companies_w_most_jobs = duckdb.sql(f'''
                  with cte as (
                  select *, 
                  from companies_breakdown
                  limit {num_companies})

                  select *
                  from cte

                  union 

                  select 'Other' as company, cast(sum(counts) as INTEGER) as count
                  from companies_breakdown 
                  where company not in (select company from cte)

                  order by company
                  ''').df()
   return companies_w_most_jobs
               
def categorize_jobs_by_title(title):
   buckets = {
      "Data Engineer":                 [r"\bdata engineer\b", r"\bengineer, data\b", r"\analytics engineer\b"],
      "Data Scientist":                [r"\bdata scientist\b", r"\bscientist\b"],
      "Data/Business Analyst":         [r"\bdata analyst\b", r"\banalyst\b", r"\banalytics\b"],
      "ML/AI Engineer":                [r"machine learning", r"\bml\b", r"\bai\b"],
      "Software Engineer":             [r"\bsoftware engineer\b", r"\bengineer, software\b"]
   }
   text = title.lower()
   for bucket, patterns in buckets.items():
      for pat in patterns:
         if re.search(pat, text):
            return bucket
         
   return 'Other'

data['general_title'] = data['title'].apply(categorize_jobs_by_title)

print(data[data['general_title'] == 'Other'])