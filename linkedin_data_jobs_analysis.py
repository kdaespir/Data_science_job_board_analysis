import pandas as pd
import duckdb
import re
from langdetect import detect
from deep_translator import GoogleTranslator
from geotext import GeoText
import geonamescache
import pycountry
import matplotlib.pyplot as plt

gc = geonamescache.GeonamesCache()
cities_dict = gc.get_cities() 
city_to_cc = {
   info['name']: info['countrycode']
   for info in cities_dict.values()
}

data = pd.read_csv('clean_jobs.csv')

# Gives the number of null columns in each feature
print(data.isnull().sum(),data.shape)

#employment type and worktype was all NaN in this dataset and can be dropped
data.drop(['employment_type', 'work_type'], inplace=True, axis=1 )

data['location'] = data['location'].str.replace(r'\b(?:area|greater|metropolitan|urban|rural)\b', '', regex=True, flags=re.IGNORECASE)
data['location'] = data['location'].str.strip()


def detect_replace(text):
   if detect(text) != 'en':
      return GoogleTranslator(source="auto", target="en").translate(text)
   else:
      return text

# translates the text in the title and description columns to english
# data['title'] = data['title'].apply(detect_replace)
# data['description'] = data['description'].apply(detect_replace) 


def jobs_by_company(num_companies=4):
   """
   This function takes an integer and returns a table with a number of rows equal to that of the interger plus 1.
   where each row is a company and the number of jobs they have posted. the remaining column is an aggregation that shows
   the total number summed from the companies not displayed in the table.
   """
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
   _ = jobs_by_company().sort_values(by=['counts'], ascending=True)
   _.plot(x='company', y='counts', kind='barh', title="Number of Job Posts By Company", xlabel="Number of Job Posts")
   plt.show()
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

def breakdown_by_title():

   agg = duckdb.sql("""
            select general_title as job_title, count(id) as counts
            from data
            group by general_title
            order by counts desc
            """).df()
   agg.plot(x='job_title', y='counts', kind='barh', title='Number of Job Posts by Job Title', xlabel="Number of Job Posts")
   plt.show()
   return agg 

def jobs_by_date():
   agg = duckdb.sql("""
                    select date_posted, count(id) as counts
                    from data
                    group by date_posted 
                    order by counts desc
                    """).df()
   agg.plot(x='date_posted', y='counts', kind='line', title='Number of Job Posts by date', xlabel="Date", ylabel='Number of job posts')
   plt.show()
   return agg

def jobs_by_date_title():
   agg = duckdb.sql("""
                    select general_title as title, date_posted, count(id) as counts
                    from data
                    group by general_title, date_posted
                    order by title, date_posted
                    """).df()
   return agg

def get_cities_countries(text):
   def identify_extra_countries(text):
      
      text_lis = text.split(',')
      if len(text_lis[-1].lstrip()) == 2:
         return "United States"
      elif text_lis[-1].lstrip().lower() in ['uae', 'united arab emirates']:
         return "United Arab Emirates"
      else:
         return ""

   place = GeoText(text)
   city_list = place.cities
   country_list = place.countries

   city = city_list[0] if city_list else ''
   country = country_list[0] if country_list else identify_extra_countries(text)
   return pd.Series({'city': city, 'country': country})

data[['city', 'country']] = data['location'].apply(get_cities_countries)

def catch_missing_city_country(location, city, country):
   if location.strip() in city_to_cc.keys():
      if country == '':
         country_out = pycountry.countries.get(alpha_2=city_to_cc.get(location.strip())).name
      else:
         country_out = country[:]
      if city == '':
         city_out = location
      else:
         city_out = city[:]
      return pd.Series({'city': city_out, 'country': country_out})
   elif city.strip() in city_to_cc.keys():
         city_out = city[:].strip()
         country_out = pycountry.countries.get(alpha_2=city_to_cc.get(city.strip())).name
         return pd.Series({'city': city_out, 'country': country_out})
   elif location.split('-')[0].strip() in city_to_cc.keys():
      if country == '':
         country_out = pycountry.countries.get(alpha_2=city_to_cc.get(location.split('-')[0].strip())).name
      else:
         country_out = country[:]
      if city == '':
         city_out = location.split('-')[0].strip()
      else:
         city_out = city[:]
      return pd.Series({'city': city_out, 'country': country_out})
   elif location.split(',')[0].strip() in city_to_cc.keys():
      if country == '':
         country_out = pycountry.countries.get(alpha_2 = city_to_cc.get(location.split(',')[0].strip())).name
      else:
         country_out = country[:]
      if city == '':
         city_out = location.split(',')[0].strip()
      else:
         city_out = city[:]
      return pd.Series({'city': city_out, 'country': country_out})
   else:
      return pd.Series({'city': city, 'country': country})

data[['city', 'country']] = data.apply(lambda row: catch_missing_city_country(row.location, row.city, row.country), axis=1)
data['city'] = data['city'].replace('', 'Unknown')

def breakdown_by_city(num_cities=5):
   agg = duckdb.sql(f"""
                    with cte as (
                    select city, count(id) as counts
                    from data
                    group by city
                    order by counts desc
                    limit {num_cities})

                    select *
                    from cte

                    union

                    select 'Other' as city, count(id) as counts
                    from data
                    where city not in (select city from cte)
                    order by counts desc
                    """).df()
   return agg

def breakdown_by_country(num_cities=4):
   agg = duckdb.sql(f"""
                    with cte as (
                    select country, count(id) as counts
                    from data
                    group by country
                    order by counts desc
                    limit {num_cities})

                    select *
                    from cte

                    union

                    select 'Other' as country, count(id) as counts
                    from data
                    where country not in (select country from cte)
                    order by counts desc
                    """).df()
   return agg

def get_pay_ranges():
   from transformers import pipeline
   qa_model = pipeline("question-answering")
   def get_qa_result(x):
      result = qa_model(question="What is the pay range for this role?", context=x)
      if result['score'] > 0.45:
         return result['answer']
      else:
         return "No Pay Range Listed"
   data["pay_range"] = data['description'].apply(lambda x: get_qa_result(x))

# get_pay_ranges()
# print(data['pay_range'])