"""
Serverless B2B Data Enrichment Pipeline

Architecture Overview:
- Orchestration: Built on Modal to enable asynchronous, serverless horizontal scaling of concurrent data processing.
- Data Extraction: Implements a two-pass architecture. Phase 1 utilizes web crawling (Bright Data/Exa) for baseline 
  fact-sheet generation. Phase 2 leverages LLMs (DeepSeek, OpenRouter) to evaluate schemas and classify business logic.
- Security: Secured via Microsoft Entra ID (OAuth2) SSO middleware, ensuring enterprise-grade access control for the web UI.
"""

import os
import time
import json
import re
from urllib.parse import urlparse
import pandas as pd
from dateutil import parser 
import modal

app = modal.App("b2b-enrichment-pipeline")

volume = modal.Volume.from_name("company-data-volume", create_if_missing=True)

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "pandas",
        "openpyxl",
        "python-dateutil",
        "openai",
        "exa_py",
        "requests",
        "fastapi",
        "python-multipart",
        "authlib",       
        "httpx",         
        "itsdangerous"   
    )
)

PHASE_2_MODEL = "deepseek-v4-pro"   
JEV_URL = "https://openrouter.ai/api/alpha/decisions"
JEV_MODEL = "~typesafe/jev-latest"
BRIGHT_DATA_ENDPOINT = "https://api.brightdata.com/datasets/v3/scrape?dataset_id=<YOUR_DATASET_ID>&notify=false&include_errors=true"

SESSION_HOURS = 8            

EXA_FACT_SHEET_SCHEMA = {
    "type": "object",
    "properties": {
        "founded_year": {
            "type": "string",
            "description": "Calendar year the company was founded."
        },
        "core_mechanics_and_b2b": {
            "type": "string", 
            "description": "Operational mechanics of the core product."
        },
        "ownership_and_platform": {
            "type": "string", 
            "description": "Platform dependency and first-party software status."
        },
        "revenue_streams": {
            "type": "string", 
            "description": "Software, Hardware, or Professional Services monetization."
        },
        "hardware_and_services_context": {
            "type": "string", 
            "description": "Context regarding physical assets or manual services."
        },
        "data_flow_and_record": {
            "type": "string",
            "description": "System of record status vs secondary data syncing."
        },
        "target_audience_and_niche": {
            "type": "string", 
            "description": "Primary industry niche."
        }
    },
    "required": [
        "founded_year", "core_mechanics_and_b2b", "ownership_and_platform", "revenue_streams", 
        "hardware_and_services_context", "data_flow_and_record", "target_audience_and_niche"
    ],
    "additionalProperties": False
}

PRODUCT_SCHEMA = {
    "type": "object",
    "properties": {
        "Core GTM Reason":              {"type": "string"},
        "Revenue Composition":          {
            "type": "string",
            "enum": [
                "Software", "Hardware", "Services",
                "Software;Hardware", "Software;Services",
                "Hardware;Services", "Software;Hardware;Services",
                ""
            ]
        },
        "Core GTM Offering":            {"type": "string", "enum": ["Software", "Hardware", "Services", "Unknown"]},
        "Company Type Reason":          {"type": "string"},
        "Company Type":                 {"type": "string", "enum": ["VMS", "HMS", "Other"]},
        "Product Characteristics Reason": {"type": "string"},
        "Product Characteristics":      {"type": "string", "enum": ["Core System", "Bolt-on Product", "Unknown"]},
        "Industry":                     {"type": "string"},
        "Vertical":                     {"type": "string"},
        "Company Summary":              {"type": "string"}
    },
    "required": [
        "Core GTM Reason", "Revenue Composition", "Core GTM Offering",
        "Company Type Reason", "Company Type",
        "Product Characteristics Reason", "Product Characteristics",
        "Industry", "Vertical", "Company Summary"
    ]
}

PRODUCT_ERROR_DICT = {
    "Core GTM Reason":              "[Pipeline Error]",
    "Revenue Composition":          "",
    "Core GTM Offering":            "Unknown",
    "Company Type Reason":          "[Pipeline Error]",
    "Company Type":                 "Other",
    "Product Characteristics Reason": "[Pipeline Error]",
    "Product Characteristics":      "Unknown",
    "Industry":                     "",
    "Vertical":                     "",
    "Company Summary":              "INSUFFICIENT CONTEXT: Pipeline error or API timeout."
}

# Truncated for confidentiality to protect corporate strategy
COMPETITOR_WATCHLIST = [
    "Competitor_A", "Competitor_B", "Competitor_C"
]

FINANCIAL_SCHEMA = {
    "type": "object",
    "properties": {
        "is_fully_acquired": {
            "type": "string",
            "description": "Boolean evaluation of full 100% acquisition status."
        },
        "total_funding_raw_string": {
            "type": "string",
            "description": "Total historical funding amount."
        },
        "latest_funding_amount_raw_string": {
            "type": "string",
            "description": "Value of the most recent funding round."
        },
        "latest_funding_date": {
            "type": "string",
            "description": "Date of the most recent funding event (YYYY-MM)."
        },
        "lead_investors": {
            "type": "string",
            "description": "List of lead PE or VC investors."
        },
        "acquired_by": {
            "type": "string",
            "description": "Parent entity name if acquired."
        },
        "acquired_date": {
            "type": "string",
            "description": "Transaction date (YYYY-MM)."
        },
        "acquired_summary": {
            "type": "string",
            "description": "Summary of acquisition terms and structure."
        },
        "pe_summary": {
            "type": "string",
            "description": "Narrative summary of private market history."
        }
    },
    "required": [
        "is_fully_acquired",
        "total_funding_raw_string", 
        "latest_funding_amount_raw_string", 
        "latest_funding_date", 
        "lead_investors",
        "acquired_by",
        "acquired_date",
        "acquired_summary",
        "pe_summary"
    ],
    "additionalProperties": False
}

FINANCIAL_ERROR_DICT = {
    "is_fully_acquired": "Unknown",
    "total_funding_raw_string": "Unknown",
    "latest_funding_amount_raw_string": "Unknown", 
    "latest_funding_date": "Unknown",
    "lead_investors": "Unknown",
    "acquired_by": "None",
    "acquired_date": "Unknown",
    "acquired_summary": "[Pipeline Error]",
    "pe_summary": "[Pipeline Error]"
}

PRODUCT_PROMPT = """
<REDACTED: 1500-word HMS, Proprietary VMS, and classification defining operational parameters prompt>
"""

# Truncated and generalized to protect proprietary vertical taxonomy matrices
JEV_INDUSTRIES = ["Industry_A", "Industry_B", "Industry_C"]
JEV_VERTICALS = ["Vertical_A", "Vertical_B", "Vertical_C"]


def clean_linkedin_url(url):
    if pd.isna(url) or not str(url).strip():
        return ""
    url = str(url).strip().lower()
    
    for subpage in ["/about", "/about/", "/life", "/life/", "/people", "/people/"]:
        if url.endswith(subpage):
            url = url.replace(subpage, "")
    
    if url.endswith("/"):
        url = url[:-1]
        
    return url

def clean_value(val):
    if isinstance(val, (list, tuple, set)):
        if not val: return ""
        val = list(val)[0]
    elif isinstance(val, dict):
        if not val: return ""
        val = str(val)

    if pd.isna(val) or val is None or str(val).strip().lower() in ["none", "null", "n/a", ""]:
        return ""
        
    return str(val).strip()

def get_clean_domain(url):
    if pd.isna(url) or not url:
        return ""
    url = str(url).strip()
    if not url.startswith("http"):
        url = "https://" + url
    parsed = urlparse(url)
    domain = parsed.netloc
    if domain.startswith("www."):
        domain = domain[4:]
    return domain

def normalize_funding_amount(raw):
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return int(raw)
    
    import re
    s = str(raw).strip().lower()
    if not s or s in ("none", "null", "unknown", "undisclosed", "n/a", ""):
        return None
        
    s = re.sub(r'[^0-9a-z.]', '', s)
    
    match = re.match(r"^([0-9]*\.?[0-9]+)([a-z]*)$", s)
    if not match:
        return None
        
    number = float(match.group(1))
    unit = match.group(2)
    
    if re.search(r'(b|bn|billion)', unit): multiplier = 1_000_000_000
    elif re.search(r'(m|mm|million)', unit): multiplier = 1_000_000
    elif re.search(r'(k|thousand)', unit): multiplier = 1_000
    else: multiplier = 1
    
    return int(round(number * multiplier))

def enforce_date_format(raw_date):
    if pd.isna(raw_date) or str(raw_date).strip().lower() in ["", "none", "null", "unknown", "undisclosed", "n/a"]:
        return None
    try:
        from datetime import datetime
        default_date = datetime.now().replace(day=1)
        return parser.parse(str(raw_date), default=default_date).strftime("%Y-%m-%d")
    except Exception:
        return None

def clean_linkedin_name(text):
    if not text:
        return ""
    
    text = str(text).strip()
    pattern = r'(?i)\b(?:inc|llc|corp|ltd|incorporated|corporation|limited)\b\.?'
    
    cleaned = re.sub(pattern, '', text)
    cleaned = re.sub(r'[,.\-\s]+$', '', cleaned)
    cleaned = re.sub(r'\s+[,.]\s*', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("Enrichment-keys")],
    max_containers=8,  
    timeout=600        
)
def process_company(row: dict) -> dict:
    """
    Worker function mapped concurrently across dataset rows.
    Executes web scraping, context generation, and proprietary taxonomy classification.
    """
    from openai import OpenAI
    from exa_py import Exa
    import requests

    DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY")
    EXA_API_KEY = os.environ.get("EXA_API_KEY")
    OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
    BRIGHT_DATA_API_KEY = os.environ.get("BRIGHT_DATA_API_KEY", "<YOUR_BRIGHT_DATA_API_KEY>")

    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com") 
    exa_client = Exa(api_key=EXA_API_KEY)
    
    def query_deepseek_json(target_name, text, prompt, schema, target_model, max_retries=5):
        user_message = f"Analyze the following JSON Fact Sheet for {target_name}:\n\n<context>\n{text}\n</context>"
        schema_instruction = f"\n\nOUTPUT SCHEMA REQUIREMENT:\n{json.dumps(schema)}"
        system_prompt = prompt + schema_instruction

        for attempt in range(max_retries):
            try:
                response = client.chat.completions.create(
                    model=target_model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.0,
                    extra_body={"thinking": {"type": "disabled"}}
                )
                return json.loads(response.choices[0].message.content)
            except Exception as e:
                error_msg = str(e)
                if ("429" in error_msg or "50" in error_msg) and attempt < (max_retries - 1):
                    wait_time = 15 * (attempt + 1)
                    time.sleep(wait_time)
                else:
                    return {"Error": error_msg}

    def query_jev_industry_vertical(fact_sheet, max_retries=8):
        state = "\n".join(f"{k}: {clean_value(fact_sheet.get(k, ''))}" for k in EXA_FACT_SHEET_SCHEMA["properties"])
        body = {"model": JEV_MODEL, "state": state, "questions": {
            "industry": {"type": "choice", "instructions": "Single best industry.", "criteria": {i: i for i in JEV_INDUSTRIES}},
            "vertical": {"type": "choice", "instructions": "Single best vertical.", "criteria": {v: v for v in JEV_VERTICALS}},
        }}
        last_error = ""
        for attempt in range(max_retries):
            try:
                resp = requests.post(JEV_URL,
                                     headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
                                     data=json.dumps(body), timeout=90)
                data = resp.json()
                answers = data.get("answers") or {}
                industry = answers.get("industry", {}).get("choice", "")
                vertical = answers.get("vertical", {}).get("choice", "")
                if industry in JEV_INDUSTRIES and vertical in JEV_VERTICALS:
                    return {"Industry": industry, "Vertical": vertical}
                last_error = str(data.get("error", data))[:200]
                if resp.status_code in (401, 402, 403):
                    break
            except Exception as e:
                last_error = str(e)[:200]
            time.sleep(2)
        return {"Error": last_error}

    headers = {
        "Authorization": f"Bearer {BRIGHT_DATA_API_KEY}",
        "Content-Type": "application/json"
    }

    raw_website = row.get('Website', '')
    raw_linkedin = row.get('Company Linkedin', '')
    
    domain = get_clean_domain(raw_website)
    target_linkedin = clean_linkedin_url(raw_linkedin)
    
    current_company_type = "Other"
    product_data = PRODUCT_ERROR_DICT.copy()
    financial_data = FINANCIAL_ERROR_DICT.copy()
    
    audit_flags = []
    company_name, employees, founded = "", "", ""
    exa_fact_sheet = None

    if not target_linkedin:
        audit_flags.append("Linkedin missing")
    else:
        payload = {"input": [{"url": target_linkedin}], "limit_per_input": None}
        try:
            response = requests.post(BRIGHT_DATA_ENDPOINT, headers=headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    data = data[0]
                company_name = data.get("name", "")
                employees = data.get("employees_in_linkedin", "")
                founded = data.get("founded", data.get("founded_year", ""))
                
                if not str(company_name).strip() or not str(employees).strip() or not str(founded).strip():
                    audit_flags.append("Linkedin data missing")
            else:
                audit_flags.append("Linkedin not responding")
        except Exception:
            audit_flags.append("Linkedin not responding")

    if not domain:
        product_data["Company Summary"] = "[Config Error]: Row missing valid URL/Domain."
        financial_data["acquired_summary"] = "[Config Error]: Row missing valid URL/Domain."
        financial_data["pe_summary"] = "[Config Error]: Row missing valid URL/Domain."
    else:
        max_exa_retries = 3
        for attempt in range(max_exa_retries):
            try:
                exa_product_response = exa_client.search(
                    query=f"Calendar year the company was founded or established, AND detailed overview of the core software product, target audience, pricing model for {domain}",
                    type="deep",
                    num_results=3,
                    include_domains=[domain],
                    output_schema=EXA_FACT_SHEET_SCHEMA
                )
                if exa_product_response and exa_product_response.output:
                    exa_fact_sheet = exa_product_response.output.content
                    break 
            except Exception:
                if attempt < max_exa_retries - 1:
                    time.sleep(3)

        if not exa_fact_sheet:
            product_data["Company Type"] = "Other"
            product_data["Company Type Reason"] = "Insufficient Context from domain extraction."
            product_data["Company Summary"] = "INSUFFICIENT CONTEXT: Could not extract business footprint from domain (crawling blocked or unindexed). Defaulted to Other."
        else:
            if not clean_value(founded):
                exa_year = clean_value(exa_fact_sheet.get("founded_year", ""))
                if re.match(r"^\d{4}$", exa_year):
                    founded = exa_year

            json_fact_sheet_string = json.dumps(exa_fact_sheet, indent=2)
            
            classifier_res = query_deepseek_json(domain, json_fact_sheet_string, PRODUCT_PROMPT, PRODUCT_SCHEMA, PHASE_2_MODEL)
            
            if "Error" in classifier_res:
                product_data["Company Type"] = "Other"
                product_data["Company Type Reason"] = "Classifier error or timeout."
                product_data["Company Summary"] = f"INSUFFICIENT CONTEXT: [API Error] {classifier_res['Error']}. Defaulted to Other."
            else:
                product_data = classifier_res

        current_company_type = product_data.get("Company Type", "Other")

        if current_company_type == "Other":
            audit_flags.append("Safety Check")
            financial_data["acquired_summary"] = f"[Pipeline Skipped]: Classified as '{current_company_type}' in Phase 2."
            financial_data["pe_summary"] = f"[Pipeline Skipped]: Classified as '{current_company_type}' in Phase 2."
            product_data["Industry"] = ""
            product_data["Vertical"] = ""
        else:
            jev_res = query_jev_industry_vertical(exa_fact_sheet) if exa_fact_sheet else {"Error": "No Exa fact sheet"}
            if "Error" in jev_res:
                product_data["Industry"] = ""
                product_data["Vertical"] = ""
                audit_flags.append(f"Jev error: {jev_res['Error']}")
            else:
                product_data["Industry"] = jev_res["Industry"]
                product_data["Vertical"] = jev_res["Vertical"]

            query_text = (
                f"Give me the funding and acquisition details of {raw_website}\n"
                f"1) What is the total funded amount?\n"
                f"2) Who are the investors or lead venture capital firms?\n"
                f"3) When was the last funding round date?\n"
                f"4) What was the last funding round amount?\n"
                f"5) When was the company acquired?\n"
                f"6) Who acquired it?\n"
                f"7) Was it a 100% full acquisition, or a merger/minority stake/rebranding?"
            )
            
            try:
                exa_response = exa_client.search(
                    query=query_text,
                    type="deep",
                    num_results=5,
                    output_schema=FINANCIAL_SCHEMA
                )
                financial_data = exa_response.output.content
                if not financial_data:
                    financial_data = FINANCIAL_ERROR_DICT.copy()
            except Exception as e:
                financial_data["acquired_summary"] = f"[API Error]: {str(e)}"
                financial_data["pe_summary"] = f"[API Error]: {str(e)}"

    product_data.pop("Error", None)
    financial_data.pop("Error", None)

    raw_total = financial_data.get("total_funding_raw_string")
    raw_latest_amount = financial_data.get("latest_funding_amount_raw_string")
    raw_latest_date = financial_data.get("latest_funding_date")
    acquired_by_str = financial_data.get("acquired_by")
    raw_acq_date = financial_data.get("acquired_date")
    raw_investors = financial_data.get("lead_investors")
    is_fully_acquired_str = financial_data.get("is_fully_acquired", "No")

    total_funding = str(normalize_funding_amount(raw_total)) if normalize_funding_amount(raw_total) else None
    latest_funding_amount = str(normalize_funding_amount(raw_latest_amount)) if normalize_funding_amount(raw_latest_amount) else None
    
    latest_funding_date = enforce_date_format(raw_latest_date) if raw_latest_date and str(raw_latest_date).lower() not in ["n/a", "none", "unknown", ""] else None
    final_acquired_date = enforce_date_format(raw_acq_date) if raw_acq_date and str(raw_acq_date).lower() not in ["n/a", "none", "unknown", ""] else None
    
    pe_name = raw_investors if raw_investors and str(raw_investors).lower() not in ["n/a", "none", "unknown", ""] else None 

    if isinstance(acquired_by_str, dict):
        acquired_by_str = acquired_by_str.get("value")
    if acquired_by_str is not None and not isinstance(acquired_by_str, str):
        acquired_by_str = str(acquired_by_str)

    if acquired_by_str and acquired_by_str.strip().lower() not in ["n/a", "none", "unknown", "", "independent", "not acquired", "false", "no"]:
        final_acquired_by = acquired_by_str.strip()
    else:
        final_acquired_by = None

    if current_company_type == "Other":
        final_pe_invested = False
        final_acquired = False
        final_acq_comp = False
    else:
        ignore_list = ["n/a", "none", "independent", "not acquired", "false", "no", "unknown", ""]
        has_valid_buyer = bool(acquired_by_str and acquired_by_str.strip().lower() not in ignore_list)
        final_acquired = bool(has_valid_buyer and str(is_fully_acquired_str).strip().title() == "Yes")
        
        final_pe_invested = bool(
            total_funding is not None or 
            latest_funding_amount is not None or 
            latest_funding_date is not None or 
            pe_name is not None
        )
        
        final_acq_comp = False
        if final_acquired and acquired_by_str:
            acquired_by_lower = acquired_by_str.lower()
            for competitor in COMPETITOR_WATCHLIST:
                if competitor.lower() in acquired_by_lower:
                    final_acq_comp = True
                    break

    if not final_acquired:
        final_acquired_date = None
        final_acquired_by = None
        
    if final_acquired:
        audit_flags.append("Acquisition check")

    summary_parts = []
    if product_data.get("Company Summary"):
        summary_parts.append(product_data["Company Summary"])
    
    acquired_summary = financial_data.get("acquired_summary", "")
    if final_acquired and acquired_summary and acquired_summary.lower() not in ["n/a", "none", "[pipeline error]"]:
        summary_parts.append(f"— Acquisition —\n{acquired_summary}")
        
    pe_summary = financial_data.get("pe_summary", "")
    if pe_summary and pe_summary.lower() not in ["n/a", "none", "[pipeline error]"]:
        summary_parts.append(f"— Funding —\n{pe_summary}")
        
    combined_summary = "\n\n".join(summary_parts)

    combined_row = row.copy()
    cleaned_company_name = clean_linkedin_name(company_name)
    
    combined_row["Company Name"]            = clean_value(cleaned_company_name)
    combined_row["Employees"]               = clean_value(employees)
    combined_row["Founded"]                 = clean_value(founded)
    
    combined_row["Company Type Reason"]     = product_data.get("Company Type Reason", "")
    combined_row["Type"]                    = product_data.get("Company Type", "Other")
    combined_row["Industry"]                = product_data.get("Industry", "")
    combined_row["Vertical"]                = product_data.get("Vertical", "")
    combined_row["Revenue Composition"]     = product_data.get("Revenue Composition", "")
    combined_row["Product Characteristics"] = product_data.get("Product Characteristics", "Unknown")
    combined_row["Company Summary"]         = combined_summary
    
    combined_row["PE Invested"]             = final_pe_invested
    combined_row["PE Name"]                 = pe_name
    combined_row["Latest Funding Date"]     = latest_funding_date
    combined_row["Latest Funding Amount"]   = latest_funding_amount
    combined_row["Total Funding"]           = total_funding
    
    combined_row["Acquired"]                = final_acquired
    combined_row["Acquired Date"]           = final_acquired_date
    combined_row["Acquired by"]             = final_acquired_by
    combined_row["Acquired by Competition"] = final_acq_comp
    
    combined_row["Audit"]                   = " | ".join(audit_flags) if audit_flags else "Verified"

    return combined_row


@app.function(
    image=image,
    volumes={"/data": volume},
    timeout=86400  
)
def cloud_orchestrator(input_filepath: str, original_filename: str):
    """
    Distributes data rows to horizontal workers and aggregates results into a final Cloud Volume artifact.
    """
    print(f"🚀 Cloud Orchestrator Started: Processing {original_filename}...")
    
    df = pd.read_excel(input_filepath)
    if "Website" not in df.columns or "Company Linkedin" not in df.columns:
        print("CRITICAL ERROR: Excel must contain both 'Website' and 'Company Linkedin' columns.")
        return
        
    rows = df.to_dict(orient="records")
    results = list(process_company.map(rows))
    
    final_df = pd.DataFrame(results)
    first_cols = ["Company Name", "Employees", "Founded"]
    remaining_cols = [c for c in final_df.columns if c not in first_cols]
    final_df = final_df[first_cols + remaining_cols]
    
    import datetime
    timestamp = datetime.datetime.now().strftime("%m-%d-%Y")
    base_name = os.path.basename(original_filename).split('.')[0]
    out_filename = f"{base_name}_Enriched_{timestamp}.xlsx"
    out_path = f"/data/{out_filename}"
    
    final_df.to_excel(out_path, index=False)
    
    if os.path.exists(input_filepath):
        os.remove(input_filepath)
        
    volume.commit()
    print(f"🎉 Batch Complete! Data secured in Cloud Volume at: {out_filename}")


@app.function(
    image=image,
    volumes={"/data": volume},
    secrets=[modal.Secret.from_name("entra-auth")],
)
@modal.asgi_app()
def ui_app():
    """
    FastAPI dashboard secured with session-based Microsoft Entra ID authentication.
    Manages file uploads, initiates background orchestration, and serves resulting datasets.
    """
    from fastapi import FastAPI, File, UploadFile, Request
    from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, JSONResponse
    from starlette.middleware.sessions import SessionMiddleware
    from starlette.middleware.base import BaseHTTPMiddleware
    from authlib.integrations.starlette_client import OAuth, OAuthError

    TENANT_ID      = os.environ.get("OAUTH_TENANT_ID", "<YOUR_TENANT_ID_HERE>")
    CLIENT_ID      = os.environ.get("OAUTH_CLIENT_ID", "<YOUR_CLIENT_ID_HERE>")
    CLIENT_SECRET  = os.environ.get("OAUTH_CLIENT_SECRET", "<YOUR_CLIENT_SECRET_HERE>")
    SESSION_SECRET = os.environ.get("SESSION_SECRET", "<YOUR_SESSION_SECRET>")
    REDIRECT_URI   = os.environ.get("REDIRECT_URI", "https://your-domain.com/auth/callback")

    oauth = OAuth()
    oauth.register(
        name="entra",
        server_metadata_url=(
            f"https://login.microsoftonline.com/{TENANT_ID}/v2.0/.well-known/openid-configuration"
        ),
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        client_kwargs={"scope": "openid profile email"},
    )

    web_app = FastAPI()

    PUBLIC_PATHS = {"/login", "/auth/callback", "/health"}

    class RequireAuth(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            if request.url.path in PUBLIC_PATHS:
                return await call_next(request)
            if not request.session.get("user"):
                return RedirectResponse(url="/login")
            return await call_next(request)

    web_app.add_middleware(RequireAuth)
    web_app.add_middleware(
        SessionMiddleware,
        secret_key=SESSION_SECRET,
        same_site="lax",            
        https_only=True,
        max_age=SESSION_HOURS * 3600,
    )

    @web_app.get("/login")
    async def login(request: Request):
        return await oauth.entra.authorize_redirect(request, REDIRECT_URI)

    @web_app.get("/auth/callback", name="auth_callback")
    async def auth_callback(request: Request):
        try:
            token = await oauth.entra.authorize_access_token(request)
        except OAuthError as exc:
            return JSONResponse(
                {"error": "authentication_failed", "detail": str(exc)}, status_code=401
            )
        claims = token.get("userinfo") or {}
        request.session["user"] = {
            "email": claims.get("preferred_username") or claims.get("email"),
            "name":  claims.get("name"),
            "oid":   claims.get("oid"),
        }
        return RedirectResponse(url="/")

    @web_app.get("/logout")
    async def logout(request: Request):
        request.session.clear()
        return RedirectResponse(url="/login")

    @web_app.get("/health")
    async def health():
        return {"ok": True}

    @web_app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        import os
        user = request.session.get("user", {})
        signed_in_as = user.get("email") or user.get("name") or ""
        user_badge = (
            f'<span style="margin-left:auto;color:#6A6A6A;font-size:13px;margin-right:15px;">{signed_in_as}</span>'
            if signed_in_as else '<span style="margin-left:auto;"></span>'
        )
        files = []
        if os.path.exists("/data"):
            files = os.listdir("/data")
        
        enriched_files = [f for f in files if f.endswith(".xlsx") and "_Enriched_" in f]
        enriched_files.sort(key=lambda x: os.path.getmtime(os.path.join("/data", x)), reverse=True)
        
        file_links = ""
        for f in enriched_files[:5]:
            file_links += f'<li style="margin-bottom:12px;"><a href="/download/{f}" style="text-decoration:none; color:#4A4A6C; font-weight:700; font-size:15px;">⬇️ Download {f}</a></li>'
                
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Generic Enterprise Dashboard</title>
            <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;700&display=swap" rel="stylesheet">
        </head>
        <body style="font-family: 'Poppins', sans-serif; margin: 0; padding: 40px; background-color: #F7F5FB; color: #6A6A6A;">
            
            <div style="background: white; padding: 40px; border-radius: 12px; box-shadow: 0 4px 20px rgba(74, 74, 108, 0.08); max-width: 800px; margin: auto;">
                
                <div style="display: flex; align-items: center; margin-bottom: 30px;">
                    <div style="background-color: #4A4A6C; color: white; width: 40px; height: 40px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 24px; margin-right: 15px;">A</div>
                    <h1 style="color: #4A4A6C; margin: 0; font-size: 28px; letter-spacing: 2px; text-transform: uppercase; font-weight: 700;">Acme Corp</h1>
                    {user_badge}
                    <a href="/logout" style="text-decoration: none; color: #4A4A6C; border: 1px solid #E0E0E0; padding: 8px 18px; border-radius: 6px; font-size: 14px; font-weight: 700; font-family: 'Poppins', sans-serif; transition: background-color 0.2s;" onmouseover="this.style.backgroundColor='#F7F5FB'" onmouseout="this.style.backgroundColor='transparent'">Log out</a>
                </div>
                
                <h2 style="color: #4A4A6C; font-size: 22px; margin-bottom: 10px; font-weight: 700;">Data Enrichment Pipeline</h2>
                <p style="font-size: 15px; margin-bottom: 25px;">Upload your Excel file below. Ensure it contains <strong>Website</strong> and <strong>Company Linkedin</strong> columns.</p>
                
                <form id="uploadForm" action="/upload" method="post" enctype="multipart/form-data">
                    <div id="drop-zone" style="padding: 40px; border: 2px dashed #4A4A6C; border-radius: 8px; text-align: center; background: #EDF8F9; cursor: pointer; transition: background-color 0.2s, border-color 0.2s;">
                        <p id="drop-text" style="font-size: 16px; color: #4A4A6C; margin-bottom: 15px; font-weight: 500; font-family: 'Poppins', sans-serif;">
                            📁 Drag & drop your Excel file here, or <span style="text-decoration: underline; color: #26B0BD;">browse</span>
                        </p>
                        <input type="file" id="fileInput" name="file" accept=".xlsx" style="display: none;" required>
                        <button type="submit" id="submitBtn" style="background-color: #26B0BD; color: white; border: none; padding: 12px 28px; font-size: 16px; font-weight: 700; border-radius: 6px; cursor: pointer; font-family: 'Poppins', sans-serif; transition: opacity 0.2s;">
                            Start Background Job
                        </button>
                    </div>
                </form>
                
                <hr style="border: 0; height: 1px; background: #E0E0E0; margin: 40px 0;">
                
                <h3 style="color: #4A4A6C; font-size: 18px; margin-bottom: 5px; font-weight: 700;">📂 Completed Files (Recent 5)</h3>
                <p style="font-size: 13px; margin-bottom: 20px;">Refresh this page later to check for newly finished files.</p>
                <ul style="list-style-type: none; padding: 0;">
                    {file_links if file_links else "<li style='color: #6A6A6A; font-style: italic;'>No completed files yet.</li>"}
                </ul>
            </div>

            <script>
                const dropZone = document.getElementById('drop-zone');
                const fileInput = document.getElementById('fileInput');
                const dropText = document.getElementById('drop-text');

                dropZone.addEventListener('click', (e) => {{
                    if (e.target.tagName !== 'BUTTON') {{
                        fileInput.click();
                    }}
                }});

                fileInput.addEventListener('change', () => {{
                    if (fileInput.files.length > 0) {{
                        dropText.innerHTML = `✅ Selected: <strong style="color:#26B0BD;">${{fileInput.files[0].name}}</strong>`;
                    }}
                }});

                ['dragenter', 'dragover'].forEach(eventName => {{
                    dropZone.addEventListener(eventName, (e) => {{
                        e.preventDefault();
                        e.stopPropagation();
                        dropZone.style.backgroundColor = '#D7EFF2';
                        dropZone.style.borderColor = '#26B0BD';
                    }}, false);
                }});

                ['dragleave', 'drop'].forEach(eventName => {{
                    dropZone.addEventListener(eventName, (e) => {{
                        e.preventDefault();
                        e.stopPropagation();
                        dropZone.style.backgroundColor = '#EDF8F9';
                        dropZone.style.borderColor = '#4A4A6C';
                    }}, false);
                }});

                dropZone.addEventListener('drop', (e) => {{
                    const dt = e.dataTransfer;
                    const files = dt.files;
                    if (files.length > 0) {{
                        fileInput.files = files;
                        dropText.innerHTML = `✅ Dropped: <strong style="color:#26B0BD;">${{files[0].name}}</strong>`;
                    }}
                }}, false);
            </script>
            
        </body>
        </html>
        """
        return html_content

    @web_app.post("/upload", response_class=HTMLResponse)
    async def upload_file(file: UploadFile = File(...)):
        contents = await file.read()
        file_path = f"/data/{file.filename}"
        
        with open(file_path, "wb") as f:
            f.write(contents)
        await volume.commit.aio()
        
        await cloud_orchestrator.spawn.aio(file_path, file.filename)
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Upload Successful - Acme Corp</title>
            <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;700&display=swap" rel="stylesheet">
        </head>
        <body style="font-family: 'Poppins', sans-serif; margin: 0; padding: 40px; background-color: #F7F5FB; text-align: center;">
            <div style="background: white; padding: 50px; border-radius: 12px; box-shadow: 0 4px 20px rgba(74, 74, 108, 0.08); max-width: 600px; margin: 50px auto;">
                <h2 style="color: #26B0BD; font-weight: 700; margin-bottom: 20px;">✅ File Received!</h2>
                <p style="color: #6A6A6A; margin-bottom: 30px;">The data pipeline has started in the background. You can safely close this window or return to the dashboard.</p>
                <a href="/" style="text-decoration: none; color: white; background-color: #4A4A6C; padding: 12px 28px; border-radius: 6px; font-weight: 700;">Return to Dashboard</a>
            </div>
        </body>
        </html>
        """

    @web_app.get("/download/{filename}")
    def download_file(filename: str):
        import os
        file_path = f"/data/{filename}"
        if os.path.exists(file_path):
            return FileResponse(path=file_path, filename=filename)
        return {"error": "File not found."}

    return web_app