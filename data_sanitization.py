from flask import Flask, jsonify, request
from airtable import Airtable
import pandas as pd
import numpy as np
import os
import pycountry
import matplotlib.pyplot as plt
import seaborn as sns
import re
# import requests  # Import the external 'requests' library


app = Flask(__name__)

# Old Airtable Configuration
BASE_ID_OLD = 'app5s8zl7DsUaDmtx'
API_KEY = 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3'  # Replace with a secure method to fetch the key
TABLE_NAME_OLD = 'profiles_raw'

# New Airtable Configuration
# BASE_ID_NEW1 = 'appTEXhgxahKgWLgx'
BASE_ID_NEW = 'app5s8zl7DsUaDmtx'
TABLE_NAME_NEW = 'profiles_cleaned'
TABLE_NAME_NEW1 = 'profiles_outreach'
TABLE_NAME_NEW2 = 'client_details'
TABLE_NAME_NEW3 = 'contacts_taippa_marketing'
API_KEY_NEW = os.getenv('AIRTABLE_API_KEY', 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3')
# API_KEY_NEW1 = os.getenv('AIRTABLE_API_KEY', 'patPgbQSC8pAg1Gbl.7ca275de5a5c8f2cf4389452e91c8f3f6c3e37bb2967c0f4cd8f41fa9d99044d')
# API_KEY_NEW1 = 'patPgbQSC8pAg1Gbl.7ca275de5a5c8f2cf4389452e91c8f3f6c3e37bb2967c0f4cd8f41fa9d99044d'


airtable_old = Airtable(BASE_ID_OLD, TABLE_NAME_OLD, API_KEY)
airtable_new = Airtable(BASE_ID_NEW, TABLE_NAME_NEW, API_KEY_NEW)
airtable_new1 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW1, API_KEY_NEW)
airtable_new3 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW3, API_KEY_NEW)
try:
    airtable_new2 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW2, API_KEY_NEW)
except Exception as e:
    print(f"Error initializing Airtable: {e}")


def record_exists_in_airtable(airtable_instance, record_data, unique_field):
    """
    Check if a record with the same unique identifier already exists in Airtable.
    """
    unique_value = record_data.get(unique_field)
    if not unique_value:
        return False

    # Search for the uniqueId field in Airtable
    search_result = airtable_instance.search(unique_field, unique_value)
    return len(search_result) > 0



def send_to_airtable_if_new(df, airtable_instance, unique_field, desired_fields=None, field_mapping=None, default_values=None, icp_to_outreach=None, icp_df=None):
  
    for i, row in df.iterrows():
        try:
            # Step 1: Convert row to dictionary
            record_data = row.dropna().to_dict()

            # Step 2: Restrict to desired fields if provided
            if desired_fields:
                record_data = {field: record_data[field] for field in desired_fields if field in record_data}

            # Step 3: Add uniqueId to record
            unique_id_value = f"{record_data.get('id', '')}_{record_data.get('email', '')}"
            record_data["unique_id"] = unique_id_value


            # Step 5: Map icp_to_outreach fields if applicable
            if icp_to_outreach and icp_df is not None:
                client_id = row.get("associated_client_id")
                if client_id:
                    matching_icp_rows = icp_df[icp_df["client_id"] == client_id]
                    if not matching_icp_rows.empty:
                        for outreach_field, icp_field in icp_to_outreach.items():
                            if icp_field in matching_icp_rows.columns:
                                record_data[outreach_field] = matching_icp_rows.iloc[0][icp_field]

            # Step 6: Apply field name mapping if provided
            if field_mapping:
                record_data = {field_mapping.get(k, k): v for k, v in record_data.items()}

            # Step 7: Remove 'created_time' field explicitly if it exists
            if 'created_time' in record_data:
                del record_data['created_time']
            
            # Step 8: Add default values if provided
            if default_values:
                for key, value in default_values.items():
                    record_data.setdefault(key, value)

            # Step 9: Check for duplicates and insert
            if not record_exists_in_airtable(airtable_instance, {"unique_id": unique_id_value}, unique_field):
                try:
                    airtable_instance.insert(record_data)
                    print(f"Record {i} inserted successfully into {airtable_instance}.")
                except Exception as e:
                    print(f"Failed to insert record {i}: {e}")
            else:
                print(f"Record {i} already exists in {airtable_instance}. Skipping insertion.")

        except Exception as e:
            print(f"Error processing record {i}: {e}")



def clean_name(df, column_name):
    
    def standardize_capitalization(text):
        if isinstance(text, str):
            text = text.strip()  # Strip whitespace
            return text.capitalize()  # Capitalizes the first letter and lowercases the rest
        return text

    # Apply the cleaning function to the specified column
    df[column_name] = df[column_name].apply(standardize_capitalization)
    return df


def process_email(email):
    """
    Processes an email to strip out any alias (e.g., test.email+alias@gmail.com should become test.email@gmail.com).
    """
    email = email.lower()  # Convert to lowercase for consistency
    email = re.sub(r'\+.*?@', '@', email)  # Remove any "+alias" before the '@' symbol
    return email

def expand_emails(df):
    rows = []
    for i, row in df.iterrows():
        emails = row['email'].split(',') if row['email'] != "Unknown" else ["Unknown"]
        for email in emails:
            email = email.strip()  # Clean up individual emails
            if email:  # Ignore empty email entries
                new_row = row.copy()
                new_row['email'] = email
                rows.append(new_row)
    
    # If no rows were added, return an empty DataFrame with 'email' column
    if not rows:
        return pd.DataFrame(columns=['email'])
    
    result_df = pd.DataFrame(rows)
    return result_df.reset_index(drop=True)  # Reset the index to avoid duplicates


  
def clean_urls(url, unique_id, column_name):
    if pd.isna(url) or not str(url).strip() or url.lower() in ["unknown", "n/a"]:
        return f"https://unknown-{ column_name}-{unique_id}.com"
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url

def clean_phone_number(x):
    if pd.isna(x) or not str(x).strip():
        return "Unknown"
    x = str(x).strip()
    if x.lower() == "unknown":
        return "Unknown"
    if x.startswith("+"):
        cleaned_number = '+' + ''.join(filter(str.isdigit, x))
    else:
        cleaned_number = ''.join(filter(str.isdigit, x))
    return cleaned_number if cleaned_number else "Unknown"



def fetch_client_details(df, airtable_instance, icp_field="associated_client_id", client_details_field="client_id"):
    """
    Fetch client details from Airtable based on matching associated_client_id in df and client_id in client_details.
    """
    client_details = []  # This will hold matched client details

    for _, row in df.iterrows():
        client_id = row.get(icp_field)
        
        if client_id:
            # Search for the client details in Airtable where client_id matches associated_client_id
            records = airtable_instance.search(client_details_field, client_id)
            
            if records:
                # Assuming we need the first match, append it to the client_details list
                client_details.append(records[0]['fields'])

    # Convert client details list to DataFrame
    client_details_df = pd.DataFrame(client_details)
    # print(client_details_df)
    
    return client_details_df

@app.route("/", methods=["GET"])
def fetch_and_update_data():
    try:
        all_records = airtable_old.get_all()

        data = [record.get('fields', {}) for record in all_records]
        record_ids = [record['id'] for record in all_records]

        if not data:
            return jsonify({"message": "No data found in the old Airtable."})

        df = pd.DataFrame(data)
        
        df = df.dropna(how='all')  

        df.to_csv("berkleyshomes_apollo.csv", index=False)
        # Replace problematic values
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)
        
        
        for column in df.select_dtypes(include=['object']).columns:
            df[column] = df[column].fillna("Unknown")

        # # Clean 'first_name'
        if 'first_name' in df.columns:
            df = clean_name(df, 'first_name')
        # # Clean 'last_name'
        if 'last_name' in df.columns:
            df = clean_name(df, 'last_name')
        

         # Clean 'email'
        if 'email' in df.columns:
            df['email'] = (
                df['email']
                .astype(str)
                .str.lower()
                .str.strip()
                .apply(lambda x: process_email(x))
            )
        
        #clean linkedin_url
        if ' linkedin_url' in df.columns:
            df[' linkedin_url'] = df.apply(
                lambda row: clean_urls(row[' linkedin_url'], row.name, ' linkedin_url'), axis=1
            )

        
        # Function to clean company website URLs



        # Function to clean the headline
        if 'headline' in df.columns:
            def clean_headline(headline):
                if pd.isna(headline):
                    return headline
                
                # Standardize capitalization (title case)
                headline = str(headline).strip().title()
                # Remove pipe symbols and extra spaces
                headline = headline.replace('|', ',')
                headline = ' '.join(headline.split())  # Strip extra spaces
                
                # Remove pipe symbols and extra spaces
                headline = headline.replace('|', ',')
                headline = ' '.join(headline.split())  # Strip extra spaces
                
                return headline

            # Apply cleaning function to the "headline" column
            df['headline'] = df['headline'].apply(clean_headline)

        #clean photo_url
        if 'photo_url' in df.columns:
            df['photo_url'] = df.apply(
                lambda row: clean_urls(row['photo_url'], row.name, 'photo_url'), axis=1
            )

        #clean twitter_url
        if 'twitter_url' in df.columns:
            df['twitter_url'] = df.apply(
                lambda row: clean_urls(row['twitter_url'], row.name, 'twitter_url'), axis=1
            )    

        #clean organization_website
        if 'organization_website' in df.columns:
            df['organization_website'] = df.apply(
                lambda row: clean_urls(row['organization_website'], row.name, 'organization_website'), axis=1
            )    

        #clean organization_linkedin
        if 'organization_linkedin' in df.columns:
            df['organization_linkedin'] = df.apply(
                lambda row: clean_urls(row['organization_linkedin'], row.name, 'organization_linkedin'), axis=1
            ) 

        #clean organization_facebook
        if 'organization_facebook' in df.columns:
            df['organization_facebook'] = df.apply(
                lambda row: clean_urls(row['organization_facebook'], row.name, 'organization_facebook'), axis=1
            ) 

        #clean organization_logo
        if 'organization_logo' in df.columns:
            df['organization_logo'] = df.apply(
                lambda row: clean_urls(row['organization_logo'], row.name, 'organization_logo'), axis=1
            ) 
        #clean organization_phone
        if 'organization_phone' in df.columns:
            df = clean_name(df, 'organization_phone')

        if 'organization_phone' in df.columns:
             df['organization_phone'] = df['organization_phone'].apply(clean_phone_number)
       


        # Duplicate rows for each email
        df = expand_emails(df)
        
        # Create uniqueId column by combining 'id' and 'email'
        df['unique_id'] = df['id'].fillna("Unknown") + "_" + df['email'].fillna("Unknown")   
         
        

        # Drop duplicates based on 'id' and 'email'
        df = df.drop_duplicates(subset=['id', 'email'])

        # Filter records with email not equal to "Unknown"
        filtered_df = df[df['email'] != "Unknown"]

        # Fetch the record from ICP_information based on the email
        campaign_field_mapping = {
            "first_name": "recipient_first_name",
            "last_name": "recipient_last_name",
            "email": "recipient_email",
            "organization_name": "recipient_company",
            # "location": "RecipientLocation",
            "title": "recipient_role",
            "organization_website": "recipient_company_website",
            "organization_short_description": "recipient_bio",
            "linkedin_url" : "linkedin_profile_url",
            # "id" : "id"
            # Add other mappings as needed
        }
       
    
           

        default_values_campaign = {
            # "cta_options" : "Schedule a quick 15-minute call to discuss how we can help GrowthTech Solutions scale personalized email outreach. At https://taippa.com/contact/ ",
            # "color_scheme" : "#000000,#ffffff,#b366cf,#6834cb",
            # "fonts" : " Headlines: Anton Body: Poppins"
        }
       
        # Fetch ICP records based on associated_client_id
        icp_df = fetch_client_details(df, airtable_new2, icp_field="associated_client_id", client_details_field="client_id")

       

        # Define field mapping for outreach_data
        icp_to_outreach_mapping = {
            "sender_email": "email",
            "sender_company": "company_name",
            "sender_name": "full_name",
            "sender_title": "job_title",
            "sender_company_website": "company_website",
            "key_benefits" : "solution_benefits",            
            "impact_metrics" : "solution_impact_examples",
            "unique_features" : "unique_features",
            "cta_options" : "cta_options",
            "color_scheme" : "color_scheme",
            "font_style" : "font_style",
            "instantly_campaign_id" : "instantly_campaign_id",
            "business_type" : "business_type"

        }

        send_to_airtable_if_new(df, airtable_new, unique_field='unique_id')
        send_to_airtable_if_new(
            filtered_df,
            airtable_new1,
            unique_field="unique_id",
            desired_fields=[
                
                "linkedin_url",
                "first_name",
                "last_name",
                "email",
                "organization_name",
                "title",
                "organization_website",
                "organization_short_description",
                "unique_id",
                "id",
                "associated_client_id",
                "employment_summary"
            ],
            field_mapping=campaign_field_mapping,
            icp_to_outreach=icp_to_outreach_mapping,
            default_values=default_values_campaign,
            icp_df=icp_df,
                    )
        

        return jsonify({"message": "Data cleaned, updated, and old records processed successfully."})

    except Exception as e:
        return jsonify({"error": f"Error fetching, processing, or deleting data: {e}"}), 500




@app.route('/post-data', methods=['GET'])
def post_data():
    return {"message": "Data received successfully"}, 200

if __name__ == "__main__":
    app.run(debug=True)


