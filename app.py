############################################# Code to Perform Smart Data Mining ############################################# 

# Package imports
import os
from flask import Flask, render_template, request, jsonify
import json
from pyairtable import Table,Api
import requests
import openai
import sys
from lead_magnet_pdf_generation import generate_lead_magnet_pdf
from data_sanitization import fetch_and_update_data, update_email_opens, collect_lead_magnet

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
        buyer_examples = record['fields']['buyer_examples']
        return solution_benefits,unique_features,solution_impact_examples,domain,buyer_criteria,buyer_examples
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

def qualify_lead(persona_details,solution_benefits,unique_features,solution_impact_examples,domain,buyer_criteria,buyer_examples):
    try:
        title = persona_details['title']
        headline = persona_details['headline']
        country = persona_details['country']
        city = persona_details['city']
        departments = persona_details['departments']
        subdepartments = persona_details['subdepartments']
        functions = persona_details['functions']
        employment_summary = persona_details['employment_summary']

        new_prompt = f"""
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

        Below are examples of warm lead profiles. These are generic examples, which means this is not exactly how they should look:

        1. Title: Director of Operations
        LinkedIn Headline: "Operations Leader | Expert in Streamlining Business Processes and Driving Growth in FMCG"
        Country: UAE
        City: Dubai
        Departments worked: Operations, Supply Chain, Business Development
        Subdepartments worked: Logistics, Procurement, Process Improvement
        Functions: Strategic Planning, Operational Efficiency, Supply Chain Management
        Employment Summary: With over 12 years of experience leading operations in multinational FMCG companies, this professional specializes in optimizing supply chains, improving operational efficiency, and driving business growth. Actively exploring opportunities in both professional growth and real estate investments in Dubai.

        2. Title: Senior Marketing Manager
        LinkedIn Headline: "Marketing Expert | Specializing in Digital Campaigns and Brand Growth"
        Country: UK
        City: Dubai
        Departments worked: Marketing, Digital Strategy
        Subdepartments worked: Campaign Management, Social Media
        Functions: Brand Development, Consumer Engagement, Campaign Strategy
        Employment Summary: Senior marketing professional with 10+ years of experience in consumer goods. Leads brand strategies and digital campaigns. Constantly on the lookout for ways to expand professional expertise in new sectors and network with like-minded professionals.

        3. Title: Chief Financial Officer (CFO)
        LinkedIn Headline: "CFO | Driving Financial Strategy and Corporate Growth"
        Country: USA
        City: Dubai
        Departments worked: Finance, Investment Strategy
        Subdepartments worked: Financial Planning, Budgeting, Risk Management
        Functions: Financial Strategy, Risk Mitigation, Investment Portfolio Management
        Employment Summary: Seasoned CFO with extensive expertise in corporate finance, budgeting, and risk management. Currently leading financial transformation at a large multinational firm. Focused on innovative growth strategies and expanding the company’s market share through strategic investments.

        4. Title: Head of Business Development
        LinkedIn Headline: "Strategic Leader | Fostering Business Expansion and Revenue Growth"
        Country: India
        City: Dubai
        Departments worked: Business Development, Sales
        Subdepartments worked: Market Expansion, Client Relations
        Functions: Business Growth, Strategic Partnerships
        Employment Summary: Experienced leader in business development for a multinational corporation. Focused on driving new market strategies and partnerships. Passionate about optimizing processes and improving operational efficiencies across various sectors.

        5. Title: Senior Legal Counsel
        LinkedIn Headline: "Legal Expert | Specializing in Corporate Law and Real Estate Transactions"
        Country: UAE
        City: Dubai
        Departments worked: Legal
        Subdepartments worked: Corporate Law, Real Estate Law
        Functions: Legal Advisory, Contract Negotiations, Corporate Governance
        Employment Summary: Senior legal counsel specializing in corporate law, with significant experience in advising multinational firms on legal matters. Adept in negotiation, risk management, and handling large-scale mergers and acquisitions.

        6. Title: Chief Operating Officer (COO)
        LinkedIn Headline: "Operations Leader | Transforming Business Operations for Maximum Efficiency"
        Country: Saudi Arabia
        City: Dubai
        Departments worked: Operations
        Subdepartments worked: Process Improvement, Supply Chain Management
        Functions: Operational Strategy, Efficiency Optimization
        Employment Summary: COO at a regional logistics leader, driving operational strategies and efficiency improvements. Specializes in supply chain optimization and leading cross-functional teams to deliver strategic objectives.

        7. Title: Senior Product Manager
        LinkedIn Headline: "Product Innovator | Leading Digital Transformation in Technology"
        Country: USA
        City: Dubai
        Departments worked: Product Management
        Subdepartments worked: Digital Products, Strategy
        Functions: Product Lifecycle Management, Digital Transformation
        Employment Summary: Senior product manager with a proven track record of leading successful tech innovations. Focused on enhancing user experience and developing cutting-edge products. Engages in market research and user-centered design principles.

        8. Title: Vice President of Marketing
        LinkedIn Headline: "Brand Strategist | Expert in Expanding Market Presence and Consumer Engagement"
        Country: Canada
        City: Dubai
        Departments worked: Marketing
        Subdepartments worked: Brand Strategy, Advertising
        Functions: Brand Positioning, Consumer Insights
        Employment Summary: Vice President of marketing at a global brand, overseeing strategies to build brand presence and foster long-term consumer loyalty. Strong expertise in analyzing market trends, developing campaigns, and executing cross-functional growth plans.

        9. Title: Head of Corporate Strategy
        LinkedIn Headline: "Strategy Leader | Helping Businesses Scale Through Innovation"
        Country: UK
        City: Dubai
        Departments worked: Strategy
        Subdepartments worked: Corporate Development
        Functions: Market Analysis, Corporate Growth
        Employment Summary: Senior executive leading corporate strategy for a large multinational. Focuses on analyzing market conditions, forecasting growth opportunities, and spearheading strategic initiatives to drive business performance.

        10. Title: Senior IT Director
        LinkedIn Headline: "Technology Leader | Delivering IT Solutions that Empower Global Operations"
        Country: India
        City: Dubai
        Departments worked: IT, Technology
        Subdepartments worked: System Integration, IT Infrastructure
        Functions: IT Strategy, Systems Optimization
        Employment Summary: Senior IT director in charge of overseeing complex technology initiatives and integrating cutting-edge systems. Known for improving efficiency and reducing costs through innovative IT solutions.

        11. Title: Senior Economist
        LinkedIn Headline: "Economist | Analyzing Market Trends and Forecasting Economic Growth"
        Country: Egypt
        City: Dubai
        Departments worked: Economics, Research
        Subdepartments worked: Economic Forecasting, Market Research
        Functions: Market Insights, Economic Forecasting
        Employment Summary: Senior economist with expertise in economic forecasting and market analysis. Advises global financial institutions on emerging market trends and provides insights into global economic shifts.

        12. Title: Senior Vice President of Sales
        LinkedIn Headline: "Sales Leader | Accelerating Revenue and Expanding Global Market Reach"
        Country: South Africa
        City: Dubai
        Departments worked: Sales
        Subdepartments worked: Sales Strategy, Regional Sales
        Functions: Revenue Generation, Team Leadership
        Employment Summary: SVP of sales at a multinational firm, driving revenue growth through innovative sales strategies. Focuses on team leadership and market expansion across various sectors.

        13. Title: Director of Finance
        LinkedIn Headline: "Finance Executive | Focused on Investment Strategy and Business Growth"
        Country: UAE
        City: Dubai
        Departments worked: Finance, Investment
        Subdepartments worked: Asset Management, Risk Assessment
        Functions: Financial Strategy, Portfolio Management
        Employment Summary: A finance executive leading strategic initiatives for capital growth and risk management. Expertise in budgeting, forecasting, and developing comprehensive investment strategies for multinational corporations.

        14. Title: Head of Operations
        LinkedIn Headline: "Operations Expert | Driving Operational Excellence and Efficiency"
        Country: USA
        City: Dubai
        Departments worked: Operations
        Subdepartments worked: Process Optimization, Logistics
        Functions: Operational Planning, Risk Management
        Employment Summary: COO of a multinational firm specializing in operational strategies and logistics. Focuses on continuous improvement and system integration for enhancing productivity.

        15. Title: Chief Executive Officer (CEO)
        LinkedIn Headline: "CEO | Leading Companies to Success with Vision and Innovation"
        Country: Saudi Arabia
        City: Dubai
        Departments worked: Executive Leadership
        Subdepartments worked: Business Strategy, Corporate Governance
        Functions: Corporate Leadership, Business Strategy
        Employment Summary: CEO of a leading tech company, transforming business models to meet market demands. Strong focus on global expansion and forging new partnerships to drive growth.

        16. Title: Senior Architect
        LinkedIn Headline: "Architect | Designing Innovative, Sustainable Spaces for Modern Living"
        Country: Lebanon
        City: Dubai
        Departments worked: Architecture, Design
        Subdepartments worked: Urban Planning, Sustainable Design
        Functions: Design Leadership, Project Management
        Employment Summary: Senior architect with a passion for sustainable urban design. Leading projects that blend modern aesthetics with environmentally-friendly practices.

        17. Title: Senior Consultant
        LinkedIn Headline: "Consulting Expert | Helping Businesses Achieve Operational Excellence"
        Country: Jordan
        City: Dubai
        Departments worked: Consulting
        Subdepartments worked: Business Strategy, Operations
        Functions: Client Engagement, Strategic Planning
        Employment Summary: Senior consultant helping businesses streamline operations and implement cost-saving strategies. Specializes in corporate strategy, market analysis, and risk management.

        18. Title: VP of Marketing and Sales
        LinkedIn Headline: "Marketing & Sales Leader | Creating Value through Strategic Campaigns"
        Country: UAE
        City: Dubai
        Departments worked: Marketing, Sales
        Subdepartments worked: Marketing Campaigns, Consumer Relations
        Functions: Brand Strategy, Revenue Growth
        Employment Summary: Leading sales and marketing efforts to drive customer engagement and business growth. Focused on innovative solutions and market expansion strategies.

        Now, based on the provided details, does this person match the profile of a 'warm' lead for our company?
        Answer with **Yes/No** and provide a brief justification.
        """
       
        lead_prompt = f"""
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

        Below are examples of warm lead profiles. These are generic examples, which means this is not exactly how they should look:
        {buyer_examples}

        If no examples are provided, you can use the already existing information

        Now, based on the provided details, does this person match the profile of a 'warm' lead for our company?
        Answer with **Yes/No** and provide a brief justification.
        """

        # Send the request to OpenAI GPT-4
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": lead_prompt}
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

def people_search(query_params,client_id,qualify_leads):
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
        solution_benefits,unique_features,solution_impact_examples,domain,buyer_criteria,buyer_examples = fetch_client_details(client_id)
        print(f"\n------------Initiating Persona Data Fetch Iteration------------")
        for contact in data['people']:
            apollo_id = contact['id']
            unique_value = apollo_id
            persona_details=parse_people_info(contact)
            if qualify_leads:
                qualification_status = qualify_lead(
                    persona_details=persona_details,
                    solution_benefits=solution_benefits,
                    unique_features=unique_features,
                    solution_impact_examples=solution_impact_examples,
                    domain=domain,
                    buyer_criteria=buyer_criteria,
                    buyer_examples=buyer_examples
                    )
                if not qualification_status:
                    print(f"\n------------Lead Disqualified------------")
                    print('Skipping the entry...')
                    continue
                print(f"\n------------Lead Qualified------------")
            else:
                print(f"Skipping lead qualification...")
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

@app.route("/update-email-opens", methods=["GET"])
def update_email_opens_clicked():
    try:
        response = update_email_opens()
        return response
    except Exception as e:
        execute_error_block(f"Error occured while counting email opened and email clicked {e}")

@app.route("/collect_lead_magnet", methods=["GET"])
def collect_lead_magnet_details():
    try:
        user_id = request.args.get('user_id', default='', type=str)
        print(f"user_id : {user_id}")
        if user_id:
            response = generate_lead_magnet_pdf(user_id)
        else:
            response = "Please provide a valid user_id"
        return response
    except Exception as e:
        execute_error_block(f"Error occured while collecting lead magnet {e}")

def test_run_pipeline(test_run_id,client_id):
    try:
        record_exists = unique_key_check_airtable(column_name='id',unique_value=test_run_id)
        if record_exists:
            print(f'Record with the following id: {test_run_id} already exists. Skipping the entry...')
            return True
        enrichment_api_response = people_enrichment(test_run_id)
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
                    'email_status': "verified",
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
            print(f"\n------------Data ingestion successful for record id :{test_run_id}------------")
            response=fetch_and_update_data()
            print(response)
            print('\n------------ Data Cleaning Completed: Data Ready for Outreach ------------\n')
            return True
        else:
            print(f"Error: {enrichment_api_response.status_code}, People Enrichment API failed")
            return False
    
    except Exception as e:
        execute_error_block(f"Error occured during test run. {e}")

@app.route("/data_ingestion", methods=["GET"])
def execute_collection():
  try:
    print(f"\n------------ Started Data Collection ------------")  
    # Construct the query string dynamically
    client_id = request.args.get('client_id', default='taippa_marketing', type=str)
    test_run_id = request.args.get('test_run_id', default='', type=str)
    if not test_run_id:
        qualify_leads = request.args.get('qualify_leads', default='yes', type=str)
        job_titles = request.args.get('job_titles', default='', type=str)
        person_seniorities = request.args.get('person_seniorities', default='', type=str)
        person_locations = request.args.get('person_locations', default='', type=str)
        organization_locations = request.args.get('organization_locations', default='', type=str)
        email_status = request.args.get('email_status', default='', type=str)
        organization_num_employees_ranges = request.args.get('organization_num_employees_ranges', default='', type=str)
        page_number = int(request.args.get('page', default='1', type=str))
        results_per_page = int(request.args.get('per_page', default='1', type=str))
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
        success_status = people_search(query_params,client_id,qualify_leads)
    else:
        success_status = test_run_pipeline(test_run_id,client_id)
    return 'Successfully collected the data' if success_status else 'Failed retrieving information from Apollo.'
  except Exception as e:
    execute_error_block(f"Error occured while parsing the input. {e}")

if __name__ == '__main__':
  app.run(debug=True)
