import os
from flask import Flask, render_template, request, jsonify
import requests
import json
from pyairtable import Table
import requests
import openai
from airtable import Airtable
from pyairtable import Api  # Updated import for Api class
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

print(f"\n------------ Generate : Data Collection ------------")
print('Initializing Flask app')
app = Flask(__name__)

print(f"\n------------ Retrieving the Secret keys ------------")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
print(f"AIRTABLE_API_KEY: {AIRTABLE_API_KEY}")
print(f"OPENAI_API_KEY: {OPENAI_API_KEY}")
print(f"APOLLO_API_KEY: {APOLLO_API_KEY}")
print(f"AIRTABLE_BASE_ID: {AIRTABLE_BASE_ID}")
print(f"AIRTABLE_TABLE_NAME: {AIRTABLE_TABLE_NAME}")

print(f"\n------------ Successfully retrieved the Secret keys ------------")


def export_to_airtable(table_object,data):
    print(f"\n------------Exporting results to Airtable ------------")
    response = table_object.create(data)
    # Check if the insertion was successful
    if 'id' in response:
        print("Record inserted successfully:", response['id'])
    else:
        print("Error inserting record:", response)

def people_enrichment(apollo_id):
    print(f"\n------------Started People Enrichment API------------")
    url = f"https://api.apollo.io/api/v1/people/match?id={apollo_id}&reveal_personal_emails=false&reveal_phone_number=false"
    proxies = {
        'http': None,
        'https': None
    }
    headers = {
        "accept": "application/json",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "x-api-key": APOLLO_API_KEY
    }

    # response = requests.post(url, headers=headers)
    response = requests.post(url, headers=headers, proxies=proxies)
    print(response.json())
    print(f"------------Completed People Enrichment API------------")
    return response


def people_search(page_number,results_per_page):
  print(f"\n------------Started People Search API ------------")
  url = f"https://api.apollo.io/api/v1/mixed_people/search?person_titles[]=marketing%20manager&person_titles[]=marketing%20director&person_locations[]=Dubai%2C%20United%20Arab%20Emirates&person_seniorities[]=ceo&person_seniorities[]=cmo&person_seniorities[]=director&organization_locations[]=Dubai%2C%20United%20Arab%20Emirates&contact_email_status[]=verified&contact_email_status[]=likely%20to%20engage&organization_num_employees_ranges[]=1%2C10&organization_num_employees_ranges[]=11%2C20&organization_num_employees_ranges[]=21%2C50&page={page_number}&per_page={results_per_page}"
  proxies = {
        'http': None,
        'https': None
  }
  headers = {
      "accept": "application/json",
      "Cache-Control": "no-cache",
      "Content-Type": "application/json",
      "x-api-key": "cHfCHoGRt798OuSynPZ7Mg"
  }

#   response = requests.post(url, headers=headers)
  response = requests.post(url, headers=headers, proxies=proxies)
  api = Api(AIRTABLE_API_KEY)
  airtable_obj = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

  if response.status_code == 200:
      print(f"\n------------ People Search API completed successfully ------------")
      data = response.json()
      for contact in data['people']:
          apollo_id = contact['id']
          data_org = contact
          column_name = 'id'  # Column name to search in
          unique_value = apollo_id  # The value you want to omit
    
          # Retrieve all records from the table
          records = airtable_obj.all()
        #   print(f'records : {records}')

          # Filter out the records where the value in the column matches the value to omit
          record_exists = any(record['fields'].get(column_name) == unique_value for record in records)    
          if record_exists:
            print(f'Record {apollo_id} already exists. Skipping the entry...')
            continue   

          enrichment_api_response = people_enrichment(apollo_id)
          if enrichment_api_response.status_code == 200:
              data = enrichment_api_response.json()
              data=data['person']
              response = openai.ChatCompletion.create(
              model="gpt-3.5-turbo",  # or "gpt-4" for more advanced results
              messages=[
                  {"role": "system", "content": "You are an expert at text summarization."},
                  {"role": "user", "content": f"Please shorten this description: {data['employment_history']}"}
              ],
              max_tokens=100  # Adjust based on the desired length of the output
              )

              employment_summary = response['choices'][0]['message']['content']
              
              data_dict = {
                  'id': data['id'],
                  'first_name': data['first_name'],
                  'last_name': data['last_name'],
                  'name': data['name'],
                  'email': data['email'],
                  'linkedin_url': data['linkedin_url'],
                  'title': data['title'],
                  'seniority': data['seniority'],
                  'headline': data['headline'],
                  'is_likely_to_engage': str(data['is_likely_to_engage']),
                  'photo_url': data['photo_url'],
                  'email_status': contact['email_status'],
                  'twitter_url': data['twitter_url'],
                  'github_url': data['github_url'],
                  'facebook_url': data['facebook_url'],
                  'employment_history': str(data['employment_history']),
                  'employment_summary':str(employment_summary),
                  'organization_name': data['organization']['name'],
                  'organization_website': data['organization']['website_url'],
                  'organization_linkedin': data['organization']['linkedin_url'],
                  'organization_facebook': data['organization']['facebook_url'],
                  'organization_primary_phone': str(data['organization']['primary_phone']),
                  'organization_logo': data['organization']['logo_url'],
                  'organization_primary_domain': data['organization']['primary_domain'],
                  'organization_industry': data['organization']['industry'],
                  'organization_estimated_num_employees': str(data['organization']['estimated_num_employees']),
                  'organization_phone': data['organization']['phone'],
                  'organization_city': data['organization']['city'],
                  'organization_state': data['organization']['state'],
                  'organization_country': data['organization']['country'],
                  'organization_short_description': data['organization']['short_description'],
                  'organization_technology_names': str(data['organization']['technology_names'])
              }

              export_to_airtable(airtable_obj,data_dict)
              print('~~~~~~~~~People enrichment successful~~~~~~~~~~~~')
          else:
              print(f"Error: {enrichment_api_response.status_code}, People Enrichment API failed")
              return False
      return True  
  else:
      print(f"\n------------ ERROR : People Search API Failed ------------")
      return False

def test_connection(page_number, results_per_page):
    print('-----Starting the testing setup-----')
    url = f"https://api.apollo.io/api/v1/mixed_people/search?page={page_number}&per_page={results_per_page}"
    headers = {
        "accept": "application/json",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "x-api-key": "ti7hoaAD9sOc5DGzqZUk-Q"
    }

    # Disable proxy for this request
    proxies = {
        'http': None,
        'https': None
    }

    # Use a session with retry mechanism
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    print('Sending post request for People search API testing.......')
    response = session.post(url, headers=headers, proxies=proxies)
    print(f'Completed request....... Response : {response}')

    if response.status_code == 200:
        data = response.json()
        # Process the data
        print('Successfully accessed people search API')
        return True
    else:
        print(f"Error: {response.status_code}, People Search API failed")
    return False

@app.route("/testing", methods=["GET"])
def testing():
    print('-----------Sample Testing Module------')
    return 'Testing Successful for endpoint /testing'

@app.route("/apollo_check", methods=["GET"])
def execute_collection():
  print(f"\n------------ Started Data Collection ------------")  
  page_number = 10
  results_per_page = 1
#   job_titles = []
#   locations = []
#   organization_num_employees_ranges = [1,10]
#   seniority = []
#   email_type = []
#   success_status = people_search(page_number,results_per_page)
  success_status = test_connection(page_number,results_per_page)
  return 'Successfully collected the data' if success_status else 'Failed retrieving information from Apollo.'

if __name__ == '__main__':
  app.run(debug=True)
