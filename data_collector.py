# import pandas as pd
# import numpy as np
import os
import re
import json
from pyairtable import Table,Api

# New Airtable Configuration
AIRTABLE_BASE_ID = 'app5s8zl7DsUaDmtx'
SOURCE_TABLE = 'profiles_cleaned'
LEAD_MAGNET_TABLE = 'lead_magnet_details'
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY', 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3')

AIRTABLE_RAW = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, SOURCE_TABLE)
# AIRTABLE_LEAD_MAGNET = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, LEAD_MAGNET_TABLE)

def fetch_user_details(user_table,user_id):
    records = user_table.all(formula=f"{{email}} = '{user_id}'")
    # print(records)
    if records:
        return {
            "id": str(records[0]['fields']['id']),
            "name": str(records[0]['fields']['name']),
            "email": str(records[0]['fields']['email']),
            "organization_phone": str(records[0]['fields']['organization_phone']),
            "title": str(records[0]['fields']['title']),
            "organization_name": str(records[0]['fields']['organization_name']),
            "linkedin_url": str(records[0]['fields']['linkedin_url']),
            "associated_client_id": str(records[0]['fields']['associated_client_id']),
            "employment_summary": str(records[0]['fields']['employment_summary']),
            "organization_industry": str(records[0]['fields']['organization_industry'])
        }
        # return id,name,email,phone,job_title,company_name,linkedin_profile_url,associated_client_id,employment_summary,organization_industry
    else:
        print("User id doesn't exist")
        return None

# function to export data to Airtable
def export_to_airtable(data):
    try:
        print(f"\n------------Exporting results to Airtable ------------")
        api = Api(AIRTABLE_API_KEY)
        airtable_obj = api.table(AIRTABLE_BASE_ID, LEAD_MAGNET_TABLE)
        response = airtable_obj.create(data)
        # Check if the insertion was successful
        if 'id' in response:
            print("Record inserted successfully:", response['id'])
        else:
            print("Error inserting record:", response)
    except Exception as e:
        print(f"Error occured while exporting the data to Airtable. {e}")

def collect_information(user_id):
    try:
        # Fetch data from Airtable
        user_details = fetch_user_details(AIRTABLE_RAW,user_id)
        if user_details:
            print(user_details)
            organization_industry = user_details.get('organization_industry','real_estate')
            export_to_airtable(user_details)
            return user_details
        else:
            print("No data found for the requested user")
            return None
        # matched_records = match_and_return_records(AIRTABLE_USER_ID, new3_df)

    except Exception as e:
        print(f"Exception occured while collecting user information: {e}")



if __name__ == '__main__':
    data = collect_information('nadia@cgnet.ae')
    print(data)