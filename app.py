############################################# Code to Perform Smart Data Mining ############################################# 

# Package imports
import os
from flask import Flask, render_template, request, jsonify
import json
from pyairtable import Table,Api
import requests
import openai
import sys

from data_sanitization import fetch_and_update_data

print(f"\n=============== Generate : Data Ingestion  ===============")
print('Starting the app')
app = Flask(__name__)

print(f"\n--------------- Retrieving the Secret keys ---------------")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")

APOLLO_HEADERS = {
            "accept": "application/json",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "x-api-key": APOLLO_API_KEY
        }

DATA_CLEANING_URL = "https://taipa.pythonanywhere.com/"
print(f"AIRTABLE_API_KEY: {AIRTABLE_API_KEY}")
print(f"OPENAI_API_KEY: {OPENAI_API_KEY}")
print(f"APOLLO_API_KEY: {APOLLO_API_KEY}")
print(f"AIRTABLE_BASE_ID: {AIRTABLE_BASE_ID}")
print(f"AIRTABLE_TABLE_NAME: {AIRTABLE_TABLE_NAME}")

print(f"\n----------------------------------------------------------")

def execute_error_block(error_message):
    print('============== ERROR BLOCK ==============')
    print(error_message)
    print(f"\n------------Stopping the program ------------")
    sys.exit()

def fetch_client_details(client_id):
    try:
        print(f"\n------------Fetching Client Details------------")
        api = Api(AIRTABLE_API_KEY)
        airtable_obj = api.table(AIRTABLE_BASE_ID, "client_details")
        record = airtable_obj.all(formula=f"{{client_id}} = '{client_id}'")[0]
        solution_benefits = record['fields']['solution_benefits']
        unique_features = record['fields']['unique_features']
        solution_impact_examples = record['fields']['solution_impact_examples']
        domain = record['fields']['domain']
        buyer_criteria = record['fields']['buyer_criteria']
        return solution_benefits,unique_features,solution_impact_examples,domain,buyer_criteria
    except Exception as e:
        execute_error_block(f"Error occured while fetching client details. {e}")

# function to export data to Airtable
def export_to_airtable(data):
    try:
        print(f"\n------------Exporting results to Airtable ------------")
        api = Api(AIRTABLE_API_KEY)
        airtable_obj = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        response = airtable_obj.create(data)
        # Check if the insertion was successful
        if 'id' in response:
            print("Record inserted successfully:", response['id'])
        else:
            print("Error inserting record:", response)
    except Exception as e:
        execute_error_block(f"Error occured while exporting the data to Airtable. {e}")

def unique_key_check_airtable(column_name,unique_value):
    try:
        api = Api(AIRTABLE_API_KEY)
        airtable_obj = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        records = airtable_obj.all()
        return any(record['fields'].get(column_name) == unique_value for record in records) 
    except Exception as e:
        execute_error_block(f"Error occured while performing unique value check in airtable. {e}")

def parse_people_info(data):
    try:
        print('----------Parsing the data input --------------]')
        employment_history = data['employment_history']
        response = openai.ChatCompletion.create(
                        model="gpt-3.5-turbo",  # or "gpt-4" for more advanced results
                        messages=[
                            {"role": "system", "content": "You are an expert at text summarization."},
                            {"role": "user", "content": f"Please summarize this description: {employment_history}"}
                        ],
                        max_tokens=100  # Adjust based on the desired length of the output
                        )
        employment_summary = response['choices'][0]['message']['content']
        parsed_people_info={
            "title":data['title'],
            "headline":data['headline'],
            "country":data['country'],
            "city":data['city'],
            "departments":data['departments'],
            "subdepartments":data['subdepartments'],
            "functions":data['functions'],
            "employment_summary":employment_summary
        }
        return parsed_people_info
    except Exception as e:
        execute_error_block(f"Error occured while parsing the data input. {e}")

def qualify_lead(persona_details,solution_benefits,unique_features,solution_impact_examples,domain,buyer_criteria):
    try:
        title = persona_details['title']
        headline = persona_details['headline']
        country = persona_details['country']
        city = persona_details['city']
        departments = persona_details['departments']
        subdepartments = persona_details['subdepartments']
        functions = persona_details['functions']
        employment_summary = persona_details['employment_summary']

        prompt = f"""

        Given the following details about a person:
        - Title: {title}
        - Headline: {headline}
        - Country: {country}
        - City: {city}
        - Departments: {departments}
        - Subdepartments: {subdepartments}
        - Functions: {functions}
        - Employment Summary: {employment_summary}

        Here is information about our company and the product we offer:
        - Solution benefits: {solution_benefits}
        - Unique features: {unique_features}
        - Solution impact: {solution_impact_examples}
        - Domain: {domain}
        - Targeted buyer criteria: {buyer_criteria}

        Does this person match the profile of a **warm lead** for our business?

        Answer **Yes/No** with a brief justification.
        """
        # Send the request to OpenAI GPT-4
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ]
        )
        # Print the response
        qualification_response = response['choices'][0]['message']['content']
        print('===============================================\n')
        print(f"Qualification status : {qualification_response}")
        print('===============================================\n')
        qualification_status=qualification_response[:5]
        return True if 'YES' in qualification_status.upper() else False
    except Exception as e:
        execute_error_block(f"Error occured while qualifying the lead. {e}")


def people_enrichment(apollo_id):
    try:
        print(f"\n------------Started Persona Data Enrichment------------")
        url = f"https://api.apollo.io/api/v1/people/match?id={apollo_id}&reveal_personal_emails=false&reveal_phone_number=false"

        response = requests.post(url, headers=APOLLO_HEADERS)
        # print(response.json())
        print(f"------------Completed Persona Data Enrichment------------")
        return response
    except Exception as e:
        execute_error_block(f"Error occured in the data enrichment layer. {e}")

def people_search(query_params,client_id):
  try:
    print(f"\n------------Started Persona Data Mining------------")
    base_url = "https://api.apollo.io/api/v1/mixed_people/search"
    url = f"{base_url}?{'&'.join(query_params)}"  
    response = requests.post(url, headers=APOLLO_HEADERS)
    print(f"Execution status code: {response.status_code}")
    if response.status_code == 200:
        print(f"\n------------Completed Persona Data Mining------------")
        data = response.json()
        # print(data['people'])
        # return True
        profiles_found = len(data['people'])
        enriched_profiles=0
        selected_profiles=0
        solution_benefits,unique_features,solution_impact_examples,domain,buyer_criteria = fetch_client_details(client_id)
        print(f"\n------------Initiating Persona Data Fetch Iteration------------")
        for contact in data['people']:
            print('---second iteration started--')
            apollo_id = contact['id']
            unique_value = apollo_id
            persona_details=parse_people_info(contact)
            qualification_status = qualify_lead(
                persona_details=persona_details,
                solution_benefits=solution_benefits,
                unique_features=unique_features,
                solution_impact_examples=solution_impact_examples,
                domain=domain,
                buyer_criteria=buyer_criteria
                )
            if not qualification_status:
                print(f"\n------------Lead Disqualified------------")
                print('Skipping the entry...')
                continue
            print(f"\n------------Lead Qualified------------")
            print(f"\n------------Data ingestion started for record id :{apollo_id}------------")
            record_exists = unique_key_check_airtable(column_name='id',unique_value=apollo_id)   
            if record_exists:
                print(f'Record with the following id: {apollo_id} already exists. Skipping the entry...')
                continue   

            enrichment_api_response = people_enrichment(apollo_id)
            enriched_profiles+=1
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
                    'associated_client_id': client_id,
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
                export_to_airtable(data_dict)
                selected_profiles+=1
                print(f"\n------------Data ingestion successful for record id :{apollo_id}------------")
            else:
                print(f"Error: {enrichment_api_response.status_code}, People Enrichment API failed")
                return False
        
        print('\n------------ Completed Data Collection, Initiating Data Cleaning------------\n')
        print(f"Total profiles found: {profiles_found}")
        print(f"Total profiles enriched: {enriched_profiles}")
        print(f"Total profiles uploaded: {selected_profiles}")
        # response = requests.get(DATA_CLEANING_URL)
        # test_sanitization_connection()
        response=fetch_and_update_data()
        print(response)
        print('\n------------ Data Cleaning Completed: Data Ready for Outreach ------------\n')
        return True  
    else:
        print(f"\n------------ ERROR : Persona Search API Failed ------------")
        return False
  except Exception as e:
    execute_error_block(f"Error occured during data ingestion. {e}")

@app.route("/testing_connection", methods=["GET"])
def testing_connection():
    try:
        print('------------Started Testing --------------')
        job_titles = request.args.get('job_titles', default='', type=str)
        print((job_titles))
        return {'Status':'Success'}
    except:
        execute_error_block(f"Error occured while testing. {e}")

def construct_query_param(key, values):
    return "&".join([f"{key}[]={value.replace(' ', '%20')}" for value in values])

@app.route("/data_sanitization", methods=["GET"])
def initialize_data_sanitization():
    try:
        response = fetch_and_update_data()
        return response
    except Exception as e:
        execute_error_block(f"Error occured while initializing data sanitization module {e}")

@app.route("/data_ingestion", methods=["GET"])
def execute_collection():
  try:
    print(f"\n------------ Started Data Collection ------------")  
    # Construct the query string dynamically
    job_titles = request.args.get('job_titles', default='', type=str)
    person_seniorities = request.args.get('person_seniorities', default='', type=str)
    person_locations = request.args.get('person_locations', default='', type=str)
    organization_locations = request.args.get('organization_locations', default='', type=str)
    email_status = request.args.get('email_status', default='', type=str)
    organization_num_employees_ranges = request.args.get('organization_num_employees_ranges', default='', type=str)
    page_number = int(request.args.get('page', default='1', type=str))
    results_per_page = int(request.args.get('per_page', default='1', type=str))
    client_id = request.args.get('client_id', default='taippa_marketing', type=str)
    x =  {"job_titles": job_titles, "person_seniorities": person_seniorities, "person_locations": person_locations, "organization_locations": organization_locations, "email_status": email_status, "organization_num_employees_ranges": organization_num_employees_ranges, "page_number": page_number, "Per page": results_per_page}
    print(f"Collected data : {x}")
    job_titles = job_titles.split(',')
    person_seniorities = person_seniorities.split(',')
    person_locations = [location for location in person_locations.strip("[]").split("],[")]
    organization_locations = [location for location in organization_locations.strip("[]").split("],[")]
    email_status = email_status.split(',')
    organization_num_employees_ranges=[value for value in organization_num_employees_ranges.strip('[]').split('],[')]
    print('\n\n') 
    x =  {"job_titles": job_titles, "person_seniorities": person_seniorities, "person_locations": person_locations, "organization_locations": organization_locations, "email_status": email_status, "organization_num_employees_ranges": organization_num_employees_ranges, "page_number": page_number, "Per page": results_per_page}
    print(f"Sanitized data : {x}")
    print('\n\n') 
    query_params = [
        construct_query_param("person_titles", job_titles),
        construct_query_param("person_seniorities", person_seniorities),
        construct_query_param("person_locations", person_locations),
        construct_query_param("organization_locations", organization_locations),
        construct_query_param("contact_email_status", email_status),
        construct_query_param("organization_num_employees_ranges", organization_num_employees_ranges),
    ]
    query_params.append(f"page={page_number}")
    query_params.append(f"per_page={results_per_page}")
    success_status = people_search(query_params,client_id)
    return 'Successfully collected the data' if success_status else 'Failed retrieving information from Apollo.'
  except Exception as e:
    execute_error_block(f"Error occured while parsing the input. {e}")

if __name__ == '__main__':
  app.run(debug=True)
