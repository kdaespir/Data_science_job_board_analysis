import pandas as pd
import duckdb
import re
from langdetect import detect
from deep_translator import GoogleTranslator
from iso3166 import countries_by_name
from geotext import GeoText
import geonamescache
import pycountry

data = pd.read_csv('clean_jobs.csv')

data['location'] = data['location'].str.replace(r'\b(?:area|greater|metropolitan)\b', '', regex=True, flags=re.IGNORECASE)
# Gives the number of null columns in each feature
# print(data.isnull().sum())

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

def breakdown_by_title():
   data['general_title'] = data['title'].apply(categorize_jobs_by_title)

   agg = duckdb.sql("""
            select general_title as job_title, count(id) as counts
            from data
            group by general_title
            order by counts desc
            """).df()
   return agg 

# data['location'] = data['location'].str.split(',')
# def get_country(loc):
#    all_countries = [x.lower() for x in list(countries_by_name.keys())]
#    all_countries.append('united kingdom')
#    all_countries.append('united states')
#    if len(loc[-1].lower().lstrip()) == 2:
#       country = 'USA'
#    elif loc[-1].lower().lstrip() in all_countries:
#       return loc[-1]
#    else:
#       return None
#    return country

# data['country'] = data['location'].apply(get_country)

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

def get_country_from_city(city, country):
   gc = geonamescache.GeonamesCache()
   cities_dict = gc.get_cities() 
   city_to_cc = {
    info['name']: info['countrycode']
    for info in cities_dict.values()
   }
   # if country == '':
   #    code = 

print(data[data['country'] == ''][['location', 'city', 'country']])


