import os
import requests

def get_cold_email_kpis(industry):
    """
    Fetches cold email outreach KPIs (Open Rate, CTR, Reply Rate) for the specified industry.
    Returns the data in this concise format without sources:
    1. **Open Rate:** XX%
    2. **Click-Through Rate (CTR):** XX%
    3. **Reply Rate:** XX%
    """
    # API endpoint and key
    url = "https://api.perplexity.ai/chat/completions"
    api_key = os.getenv("PERPLEXITY_API_KEY")  # Retrieve the API key from an environment variable

    if not api_key:
        return "Error: API key not found in environment variable PERPLEXITY_API_KEY"

    # Prompt for the Perplexity API
    prompt = f"""
    Find the industry average for the following cold email outreach KPIs in the {industry} industry:
    1. Open Rate: The percentage of recipients who open your cold email.
    2. Click-Through Rate (CTR): The percentage of recipients who click a link within your cold email.
    3. Reply Rate: The percentage of recipients who reply to your cold email.

    
    Use credible sources in the cold email outreach space and make sure the metrics and reasonable and accurate to the industry
    When providing this data:
    - If specific data for the {industry} industry is unavailable, use data from a similar industry or provide a reasonable estimate based on cold email outreach trends.
    - Do not include sources, references, or explanations.
    - Never say not specified always put in an accurate and reasonable estimate based on the industry
    Provide the data in this exact concise format:
    1. **Open Rate:** XX%
    2. **Click-Through Rate (CTR):** XX%
    3. **Reply Rate:** XX%
    
    Example outputs:
    
    1. **Open Rate:** 15.22% to 28.46%
    2. **Click-Through Rate (CTR):** 2-5%
    3. **Reply Rate:** 0.5%
    
    1. **Open Rate:** 23%
    2. **Click-Through Rate (CTR):** 2.6%
    3. **Reply Rate:** 0.9%

    """

    # Payload for the API request
    payload = {
        "model": "llama-3.1-sonar-small-128k-online",
        "messages": [
            {"role": "system", "content": "Be concise and only output the requested metrics. Do not include sources."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 300,
        "temperature": 0.2,
        "top_p": 0.9,
        "search_domain_filter": ["perplexity.ai"],
        "return_images": False,
        "return_related_questions": False,
        "search_recency_filter": "year",
        "top_k": 0,
        "stream": False,
        "presence_penalty": 0,
        "frequency_penalty": 1
    }

    # Headers with API key
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        # Send the request
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()

        # Extract and format the content
        if "choices" in data and len(data["choices"]) > 0:
            return data["choices"][0]["message"]["content"].strip()
        else:
            return "No content generated."
    except requests.exceptions.RequestException as e:
        return f"API request failed: {e}"

# Example usage
if __name__ == "__main__":
    industry = "Finance"  # Replace with the desired industry
    metrics = get_cold_email_kpis(industry)
    print(metrics)
