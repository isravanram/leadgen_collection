from flask import Flask, jsonify, request 
from airtable import Airtable
import pandas as pd
import numpy as np
import os
import re
import json


app = Flask(__name__)

# Old Airtable Configuration
BASE_ID_OLD = 'app5s8zl7DsUaDmtx'
API_KEY = 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3'  # Replace with a secure method to fetch the key
TABLE_NAME_OLD = 'profiles_raw'

# New Airtable Configuration
BASE_ID_NEW = 'app5s8zl7DsUaDmtx'
TABLE_NAME_NEW = 'profiles_cleaned'
TABLE_NAME_NEW1 = 'profiles_outreach'
TABLE_NAME_NEW2 = 'client_details'
TABLE_NAME_NEW3 = 'contacts_taippa_marketing'
TABLE_NAME_EMAIL_OPENED = 'email_opened'
TABLE_NAME_LINK_CLICKED = 'link_opened'
TABLE_NAME_METRICS = 'metrics'
TABLE_NAME_EMAIL_SENT = 'email_sent'
TABLE_NAME_REPLIES_RECIEVED = 'replies_received'
TABLE_NAME_LEAD_MAGNET = 'lead_magnet_details'
API_KEY_NEW = os.getenv('AIRTABLE_API_KEY', 'patELEdV0LAx6Aba3.393bf0e41eb59b4b80de15b94a3d122eab50035c7c34189b53ec561de590dff3')

airtable_old = Airtable(BASE_ID_OLD, TABLE_NAME_OLD, API_KEY)
airtable_new = Airtable(BASE_ID_NEW, TABLE_NAME_NEW, API_KEY_NEW)
airtable_new1 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW1, API_KEY_NEW)
airtable_new3 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW3, API_KEY_NEW)
airtable_email = Airtable(BASE_ID_NEW, TABLE_NAME_EMAIL_OPENED, API_KEY_NEW)
airtable_link = Airtable(BASE_ID_NEW, TABLE_NAME_LINK_CLICKED, API_KEY_NEW)
airtable_email_sent = Airtable(BASE_ID_NEW, TABLE_NAME_EMAIL_SENT, API_KEY_NEW)
airtable_replies_received = Airtable(BASE_ID_NEW, TABLE_NAME_REPLIES_RECIEVED, API_KEY_NEW)
airtable_metrics = Airtable(BASE_ID_NEW, TABLE_NAME_METRICS, API_KEY_NEW)
airtable_lead_magnet = Airtable(BASE_ID_NEW, TABLE_NAME_LEAD_MAGNET, API_KEY_NEW)
try:
    airtable_new2 = Airtable(BASE_ID_NEW, TABLE_NAME_NEW2, API_KEY_NEW)
except Exception as e:
    print(f"Error initializing Airtable: {e}")

def fetch_max_created_time(airtable_instance):
    """
    Fetch the maximum created_time from profiles_cleaned.
    """
    records = airtable_instance.get_all()
    created_times = [pd.to_datetime(record['createdTime']) for record in records if 'createdTime' in record]
    return max(created_times) if created_times else None

def filter_new_records(df, max_created_time):
    """
    Filters rows with created_time greater than the max_created_time.
    """
    if max_created_time is not None:
        df['created_time'] = pd.to_datetime(df['created_time'], errors='coerce')
        df = df[df['created_time'] > max_created_time]
    return df

def send_to_airtable_if_new(df, airtable_instance, unique_field, desired_fields=None, field_mapping=None, default_values=None, icp_to_outreach=None, icp_df=None):
    """
    Send new records to Airtable after filtering and mapping fields.
    """
    for i, row in df.iterrows():
        try:
            record_data = row.dropna().to_dict()

            if desired_fields:
                record_data = {field: record_data[field] for field in desired_fields if field in record_data}

            unique_id_value = f"{record_data.get('id', '')}_{record_data.get('email', '')}"
            record_data["unique_id"] = unique_id_value

            if icp_to_outreach and icp_df is not None:
                client_id = row.get("associated_client_id")
                if client_id:
                    matching_icp_rows = icp_df[icp_df["client_id"] == client_id]
                    if not matching_icp_rows.empty:
                        for outreach_field, icp_field in icp_to_outreach.items():
                            if icp_field in matching_icp_rows.columns:
                                record_data[outreach_field] = matching_icp_rows.iloc[0][icp_field]

            if field_mapping:
                record_data = {field_mapping.get(k, k): v for k, v in record_data.items()}

            if 'created_time' in record_data:
                del record_data['created_time']

            if default_values:
                for key, value in default_values.items():
                    record_data.setdefault(key, value)

            search_result = airtable_instance.search(unique_field, unique_id_value)
            if not search_result:
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

        if not data:
            return jsonify({"message": "No data found in the old Airtable."})

        df = pd.DataFrame(data)
        df = df.dropna(how='all')

        df.to_csv("berkleyshomes_apollo.csv", index=False)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)

        for column in df.select_dtypes(include=['object']).columns:
            df[column] = df[column].fillna("Unknown")

        if 'first_name' in df.columns:
            df['first_name'] = df['first_name'].str.strip().str.capitalize()
        if 'last_name' in df.columns:
            df['last_name'] = df['last_name'].str.strip().str.capitalize()

        if 'email' in df.columns:
            df['email'] = (
                df['email']
                .astype(str)
                .str.lower()
                .str.strip()
                .apply(lambda x: re.sub(r'\+.*?@', '@', x))
            )

        if 'created_time' in df.columns:
            max_created_time = fetch_max_created_time(airtable_new)
            df = filter_new_records(df, max_created_time)

        df['unique_id'] = df['id'].fillna("Unknown") + "_" + df['email'].fillna("Unknown")
        df = df.drop_duplicates(subset=['id', 'email'])
        filtered_df = df[df['email'] != "Unknown"]

        campaign_field_mapping = {
            "first_name": "recipient_first_name",
            "last_name": "recipient_last_name",
            "email": "recipient_email",
            "organization_name": "recipient_company",
            "title": "recipient_role",
            "organization_website": "recipient_company_website",
            "organization_short_description": "recipient_bio",
            "linkedin_url": "linkedin_profile_url",
        }

        default_values_campaign = {}

        icp_df = fetch_client_details(df, airtable_new2, icp_field="associated_client_id", client_details_field="client_id")

        icp_to_outreach_mapping = {
            "sender_email": "email",
            "sender_company": "company_name",
            "sender_name": "full_name",
            "sender_title": "job_title",
            "sender_company_website": "company_website",
            "key_benefits": "solution_benefits",
            "impact_metrics": "solution_impact_examples",
            "unique_features": "unique_features",
            "cta_options": "cta_options",
            "color_scheme": "color_scheme",
            "font_style": "font_style",
            "instantly_campaign_id": "instantly_campaign_id",
            "business_type": "business_type",
            "outreach_table": "outreach_table"
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


@app.route('/update-email-opens', methods=['POST'])
def update_email_opens():
    try:
        # Step 1: Fetch all records from email_opened table
        email_records = airtable_email.get_all()
        email_df = pd.DataFrame([record['fields'] for record in email_records])

        # Step 2: Count the number of emails opened for each campaign_id
        if 'campaign_id' not in email_df.columns:
            return jsonify({"error": "campaign_id column is missing in email_opened table"}), 400

        email_count = email_df['campaign_id'].value_counts().to_dict()

        # Step 3: Fetch all records from airtable_link table to count all emails clicked per campaign_id
        link_records = airtable_link.get_all()
        link_df = pd.DataFrame([record['fields'] for record in link_records])

        # Step 4: Count the total number of emails clicked for each campaign_id (not unique)
        if 'campaign_id' not in link_df.columns or 'email' not in link_df.columns:
            return jsonify({"error": "campaign_id or email column is missing in airtable_link table"}), 400

        # Count all occurrences of email for each campaign_id (instead of unique emails)
        email_click_count = link_df.groupby('campaign_id')['email'].count().to_dict()

        # Step 5: Fetch all records from airtable_email_sent table for unique email count
        email_sent_records = airtable_email_sent.get_all()
        email_sent_df = pd.DataFrame([record['fields'] for record in email_sent_records])

        # Ensure required columns exist
        if 'campaign_id' not in email_sent_df.columns or 'email' not in email_sent_df.columns:
            return jsonify({"error": "campaign_id or email column is missing in airtable_email_sent table"}), 400

        # Count unique emails for each campaign_id
        unique_email_count = email_sent_df.groupby('campaign_id')['email'].nunique().to_dict()
        # Step 6: Fetch all records from airtable_email_sent table for unique email count
        email_reply_records = airtable_replies_received.get_all()
        email_reply_df = pd.DataFrame([record['fields'] for record in email_reply_records])

        # Ensure required columns exist
        if 'campaign_id' not in email_reply_df.columns or 'email' not in email_reply_df.columns:
            return jsonify({"error": "campaign_id or email column is missing in airtable_email_sent table"}), 400

        # Count unique emails for each campaign_id
        reply_count = email_reply_df.groupby('campaign_id')['email'].nunique().to_dict()

        # Step 7: Update the metrics table with opened, clicked, and unique email counts
        for campaign_id in set(email_count.keys()).union(email_click_count.keys()).union(unique_email_count.keys()):
            # Search for the record in the metrics table
            metrics_record = airtable_metrics.search('campaign_id', campaign_id)
            if metrics_record:
                record_id = metrics_record[0]['id']
                update_data = {}

                # Update the opened count
                if campaign_id in email_count:
                    update_data["opened"] = str(email_count[campaign_id])  # Convert to string for long text

                # Update the clicked count
                if campaign_id in email_click_count:
                    update_data["clicked"] = str(email_click_count[campaign_id])  # Convert to string for long text

                # Update the unique email count
                if campaign_id in unique_email_count:
                    update_data["sequence_started"] = str(unique_email_count[campaign_id])  # Convert to string for long text

                # Update the unique email count
                if campaign_id in reply_count:
                    update_data["replies_received"] = str(reply_count[campaign_id])  # Convert to string for long text

                # Update the record in Airtable
                airtable_metrics.update(record_id, update_data)
                print(f"Updated campaign_id {campaign_id} with data: {update_data}")

        return jsonify({"message": "Opened, clicked, and unique email counts updated successfully in metrics table"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
from flask import Flask, jsonify, request
import pandas as pd

# Initialize Flask app
app = Flask(__name__)

def fetch_airtable_data(airtable_instance):
    """
    Fetch all records from Airtable and convert them into a cleaned pandas DataFrame.
    """
    records = airtable_instance.get_all()
    data = [record.get('fields', {}) for record in records]
    df = pd.DataFrame(data)
    return clean_dataframe(df)

def clean_dataframe(df):
    """
    Clean the DataFrame by replacing NaN, infinite, and out-of-range float values.
    """
    return df.replace([pd.NA, float('inf'), float('-inf')], None)

def match_and_return_records(lead_magnet_df, new3_df):
    """
    Compare the recipient_email from new3_df with the email from lead_magnet_df.
    Return matched records with id, recipient_role, and email.
    """
    matched_records = []
    for _, new3_row in new3_df.iterrows():
        recipient_email = new3_row.get('recipient_email')
        matching_row = lead_magnet_df[lead_magnet_df['email'] == recipient_email]
        if not matching_row.empty:
            # Append matched records
            matched_records.append({
                'id': new3_row.get('id'),
                'recipient_role': new3_row.get('recipient_role'),
                'email': recipient_email
            })

    # Filter duplicates by email, keeping the most complete record
    unique_records = filter_unique_records(matched_records)
    return unique_records

def filter_unique_records(records):
    """
    Filter duplicate records by email, prioritizing those with valid 'id' and 'recipient_role'.
    """
    grouped = {}
    for record in records:
        email = record.get('email')
        if not email:
            continue

        if email not in grouped:
            grouped[email] = record
        else:
            # Prioritize the record with valid fields
            if not grouped[email].get('id') and record.get('id'):
                grouped[email]['id'] = record['id']
            if not grouped[email].get('recipient_role') and record.get('recipient_role'):
                grouped[email]['recipient_role'] = record['recipient_role']

    return list(grouped.values())

def send_to_airtable(airtable_instance, records):
    """
    Update records in the Airtable instance for the corresponding email.
    """
    for record in records:
        email = record.get('email')
        if not email:
            continue

        # Find the Airtable record matching the email
        existing_records = airtable_instance.get_all(formula=f"{{email}} = '{email}'")
        if not existing_records:
            print(f"No record found for email: {email}")
            continue

        # Get the record ID of the first match
        record_id = existing_records[0]['id']

        # Update the corresponding record with the new data
        update_data = {key: value for key, value in record.items() if key != 'email'}
        airtable_instance.update(record_id, update_data)

@app.route('/collect_lead_magnet', methods=['POST'])
def collect_lead_magnet():
    """
    Endpoint to process data from both Airtable tables, match emails, and return a JSON response.
    """
    try:
        # Fetch data from Airtable
        lead_magnet_df = fetch_airtable_data(airtable_lead_magnet)
        new3_df = fetch_airtable_data(airtable_new3)

        # Match records
        matched_records = match_and_return_records(lead_magnet_df, new3_df)

        # Send matched records back to Airtable
        send_to_airtable(airtable_lead_magnet, matched_records)

        # Return matched records as a JSON response
        return jsonify(matched_records), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500







# @app.route('/collect_lead_magnet', methods=['POST'])
# def collect_lead_magnet():
#     try:
#         # Fetch all records from lead_magnet_details
#         lead_magnet_records = airtable_lead_magnet.get_all()
#         lead_magnet_emails = {
#             record['fields']['email']: record['id']
#             for record in lead_magnet_records if 'email' in record['fields']
#         }

#         # Fetch all records from contacts_taippa_marketing
#         new3_records = airtable_new3.get_all()
#         new3_data = {
#             record['fields']['recipient_email']: {
#                 'id': record['fields'].get('id'),
#                 'recipient_role': record['fields'].get('recipient_role'),
#                 'recipient_company': record['fields'].get('recipient_company')
#             }
#             for record in new3_records if 'recipient_email' in record['fields']
#         }

#         # Sync data
#         updates = []
#         for email, lead_magnet_id in lead_magnet_emails.items():
#             if email in new3_data:
#                 update_data = new3_data[email]
#                 airtable_lead_magnet.update(lead_magnet_id, update_data)
#                 updates.append({"email": email, "update": update_data})

#         return jsonify({"message": "Lead magnet synced successfully", "updates": updates}), 200

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# @app.route('/collect_lead_magnet', methods=['POST'])
# def collect_lead_magnet():
#     try:
#         # Fetch all records from Airtable
#         new3_records = airtable_new3.get_all()
#         lead_magnet_records = airtable_lead_magnet.get_all()

#         # Convert to DataFrames, ensuring IDs are included
#         new3_df = pd.DataFrame([
#             {**record['fields'], 'id': record['id']} for record in new3_records
#         ])
#         lead_magnet_df = pd.DataFrame([
#             {**record['fields'], 'record_id': record['id']} for record in lead_magnet_records
#         ])

#         # Fill NaN with empty strings for compatibility
#         new3_df.fillna('', inplace=True)
#         lead_magnet_df.fillna('', inplace=True)

#         # Merge data on email fields
#         matched_df = new3_df.merge(lead_magnet_df, left_on='recipient_email', right_on='email', how='inner')

#         # Drop duplicate records for the same recipient
#         matched_df = matched_df.drop_duplicates(subset=['record_id', 'recipient_email'])

#         # Debug: Check merged data
#         print("Matched DataFrame columns:", matched_df.columns)

#         # Update records in Airtable
#         for _, row in matched_df.iterrows():
#             update_data = {
#                 'id': row.get('id', ''),
#                 'recipient_role': row.get('recipient_role', ''),
#                 'recipient_company': row.get('recipient_company', '')
#             }

#             if not any(update_data.values()):
#                 print(f"Skipping record due to missing or empty values: {update_data}")
#                 continue

#             print(f"Updating record {row['record_id']} with data: {update_data}")

#             try:
#                 airtable_lead_magnet.update(row['record_id'], update_data)
#             except Exception as e:
#                 print(f"Failed to update record {row['record_id']}: {e}")

#         return jsonify({"message": "Lead magnet data updated successfully."})

#     except Exception as e:
#         return jsonify({"error": f"Error updating lead magnet data: {str(e)}"}), 500






@app.route('/post-data', methods=['GET'])
def post_data():
    return {"message": "Data received successfully"}, 200

if __name__ == "__main__":
    app.run(debug=True)


