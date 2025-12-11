#karmafy/karmafy/api_send_email_notification.py

from fastapi import FastAPI, HTTPException
from typing import Optional
import requests
import os
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# -----------------------
# Config - Load from environment variables
# -----------------------
# Load .env file from parent directory
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

# Azure AD Configuration - REQUIRED
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "support@applywizz.com")

# Validate required Azure credentials
if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    raise ValueError(
        "Missing required Azure AD credentials. Please set the following environment variables:\n"
        "  - AZURE_TENANT_ID\n"
        "  - AZURE_CLIENT_ID\n"
        "  - AZURE_CLIENT_SECRET"
    )

# Database configuration - REQUIRED
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError(
        "Missing required DATABASE_URL environment variable. "
        "Please set it in your .env file."
    )

APP_NAME = "Task Management"

# Job Links Threshold
JOB_LINKS_THRESHOLD = 60
TEST_EMAIL_RECIPIENT = "ramakrishna@applywizz.com.com"

# CC Recipients - comma-separated list of email addresses
# Can be set via environment variable: CC_EMAIL_RECIPIENTS="email1@example.com,email2@example.com"
CC_RECIPIENTS_STR = os.getenv("CC_EMAIL_RECIPIENTS", "")
CC_EMAIL_RECIPIENTS = [email.strip() for email in CC_RECIPIENTS_STR.split(",") if email.strip()]

# FastAPI app
app = FastAPI()


# -----------------------
# Database Connection
# -----------------------
def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")


# -----------------------
# Query All Leads Below Threshold
# -----------------------
def get_all_leads_below_threshold(threshold: int = 60, target_date: Optional[str] = None):
    """
    Query the karmafy_scoredjob table to find ALL leads with job count below threshold for a specific date.
    
    Args:
        threshold: The minimum job count threshold
        target_date: Date in 'YYYY-MM-DD' format. Defaults to today's date if not provided.
    
    Returns list of dicts with lead info and job count for the specified date.
    """
    conn = get_db_connection()
    try:
        # If no date provided, use today's date
        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = """
                SELECT 
                    l.name AS lead_name,
                    l.email AS lead_email,
                    l."apwId" AS apw_id,
                    l."yearsExp" AS years_exp,
                    jr.name AS job_role_name,
                    COUNT(sj.id) AS job_count_today,
                    -- difference calculation
                    60 - COUNT(sj.id) AS difference,
                    -- service type based on servicesOpted JSONB
                    CASE 
                        WHEN l."servicesOpted" @> '["applications"]'::jsonb 
                            THEN 'Opted for Applications'
                        WHEN l."servicesOpted" @> '["job-links"]'::jsonb 
                            THEN 'Opted for Job Links'
                        ELSE 'Not Selected'
                    END AS service_type
                FROM karmafy_lead l
                LEFT JOIN karmafy_jobrole jr 
                    ON jr.id = l."targetRoleId_id"
                LEFT JOIN karmafy_scoredjob sj 
                    ON sj.lead_id = l.id
                    AND DATE(sj."generatedAt") = CURRENT_DATE
                WHERE LOWER(l.status) IN ('active', 'in progress', 'inprogress')
                GROUP BY l.id, l.name, l.email, l."apwId", l."yearsExp", jr.name, l."servicesOpted"
                HAVING COUNT(sj.id) < 60
                ORDER BY job_count_today ASC
            """
            cursor.execute(query, (threshold, target_date, threshold))
            results = cursor.fetchall()
            
            # Convert to list of dicts
            return [dict(row) for row in results]
    
    except psycopg2.Error as e:
        print(f"❌ Database query error: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")
    finally:
        conn.close()


# Azure AD token
def get_access_token() -> str:
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    res = requests.post(token_url, data=data)
    if not res.ok:
        try:
            error_text = res.text
        except Exception:
            error_text = "<no body>"
        print("❌ Error getting access token:", res.status_code, error_text)
        raise HTTPException(status_code=500, detail="Failed to get access token from Azure AD")

    json_data = res.json()
    access_token = json_data.get("access_token")
    if not access_token:
        print("❌ No access_token in token response:", json_data)
        raise HTTPException(status_code=500, detail="No access token received from Azure AD")

    return access_token


# -----------------------
# Send mail via Graph
# -----------------------
def send_mail_via_graph(to: str, subject: str, html: str, cc: Optional[list] = None) -> None:
    """
    Send email via Microsoft Graph API.
    
    Args:
        to: Primary recipient email address
        subject: Email subject
        html: HTML email content
        cc: Optional list of CC email addresses
    """
    access_token = get_access_token()

    url = f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail"

    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html,
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": to
                    }
                }
            ],
        },
        "saveToSentItems": True,
    }
    
    # Add CC recipients if provided
    if cc and len(cc) > 0:
        payload["message"]["ccRecipients"] = [
            {"emailAddress": {"address": email}} for email in cc
        ]

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    res = requests.post(url, json=payload, headers=headers)
    if not res.ok:
        try:
            error_text = res.text
        except Exception:
            error_text = "<no body>"
        print("❌ Error sending email via Graph:", res.status_code, error_text)
        raise HTTPException(status_code=500, detail="Failed to send email via Microsoft Graph")


def build_multi_lead_email_template(
    app_name: str,
    leads_data: list,
    threshold: int,
    support_url: str = "https://dashboard.apply-wizz.com/",
) -> str:
    """Build the HTML email template with multiple leads' job links data"""
    
    # Build table rows for all leads
    leads_rows = ""
    for idx, lead in enumerate(leads_data, 1):
        lead_name = lead.get('lead_name', 'Unknown')
        lead_email = lead.get('lead_email', 'N/A')
        job_count = lead.get('job_count_today', 0)
        difference = lead.get('difference', 0)
        apw_id = lead.get('apw_id', 'N/A') or 'N/A'
        years_exp = lead.get('years_exp', 'N/A') or 'N/A'
        job_role_name = lead.get('job_role_name', 'N/A') or 'N/A'
        service_type = lead.get('service_type', 'Not Selected')
        
        leads_rows += f"""
        <tr style="border-bottom: 1px solid #e5e7eb;">
            <td style="padding: 10px 6px; text-align: center; color: #6b7280; font-weight: 600; font-size: 13px;">{idx}</td>
            <td style="padding: 10px 6px; color: #111827; font-weight: 600; font-size: 13px;">{lead_name}</td>
            <td style="padding: 10px 6px; color: #4b5563; font-family: ui-monospace, monospace; font-size: 12px;">{lead_email}</td>
            <td style="padding: 10px 6px; text-align: center; color: #6b7280; font-size: 13px;">{apw_id}</td>
            <td style="padding: 10px 6px; text-align: center; color: #2563eb; font-weight: 600; font-size: 13px;">{years_exp}</td>
            <td style="padding: 10px 6px; color: #5b21b6; font-weight: 600; font-size: 13px;">{job_role_name}</td>
            <td style="padding: 10px 6px; color: #059669; font-weight: 600; font-size: 13px;">{service_type}</td>
            <td style="padding: 10px 6px; text-align: center; font-weight: 700; color: #e11d48; font-size: 13px;">{job_count}</td>
            <td style="padding: 10px 6px; text-align: center; color: #6b7280; font-size: 13px;">{threshold}</td>
            <td style="padding: 10px 6px; text-align: center; font-weight: 700; color: #dc2626; font-size: 13px;">-{difference}</td>
        </tr>
        """
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charSet="UTF-8" />
  <title>{app_name} – Job Links Threshold Alert</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {{
      margin: 0;
      padding: 0;
      background-color: #f3f4f6;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: #111827;
    }}
    .container {{
      max-width: 1100px;
      margin: 32px auto;
      background: #ffffff;
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 18px 45px rgba(15, 23, 42, 0.12);
    }}
    .header {{
      background: #fef2f2;
      padding: 24px 32px;
      color: white;
      text-align: left;
    }}
    .title {{
      margin-top: 10px;
      font-size: 24px;
      font-weight: 700;
      color:#ef4444;
    }}
    .sub {{
      margin-top: 6px;
      font-size: 14px;
      opacity: 0.9;
      color:#ef4444;
    }}
    .body {{
      padding: 24px 32px 32px;
      font-size: 14px;
      line-height: 1.6;
    }}
    .alert-box {{
      background-color: #fef2f2;
      border-left: 4px solid #ef4444;
      padding: 16px;
      margin: 20px 0;
      border-radius: 8px;
    }}
    .table-wrapper {{
      overflow-x: auto;
      margin: 20px 0;
    }}
    table {{
      width: 100%;
      min-width: 1000px;
      border-collapse: collapse;
      background: white;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      overflow: hidden;
    }}
    th {{
      background: #f9fafb;
      padding: 10px 6px;
      text-align: left;
      font-weight: 700;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
      color: #6b7280;
      border-bottom: 2px solid #e5e7eb;
      white-space: nowrap;
    }}
    td {{
      padding: 10px 6px;
      font-size: 13px;
      border-bottom: 1px solid #f3f4f6;
    }}
    th:nth-child(1), th:nth-child(4), th:nth-child(5), th:nth-child(8), th:nth-child(9), th:nth-child(10) {{
      text-align: center;
    }}
    td:nth-child(1), td:nth-child(4), td:nth-child(5), td:nth-child(8), td:nth-child(9), td:nth-child(10) {{
      text-align: center;
    }}
    .footer {{
      padding: 16px 32px 24px;
      font-size: 11px;
      color: #9ca3af;
      text-align: center;
    }}
    .cta-button {{
      display: inline-block;
      margin-top: 18px;
      padding: 12px 24px;
      background: linear-gradient(135deg, #e11d48, #f59e0b);
      color: white !important;
      text-decoration: none;
      border-radius: 999px;
      font-weight: 600;
      font-size: 14px;
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="title">⚠️ Job Links Below Threshold Alert</div>
      <div class="sub">
        {len(leads_data)} lead(s) have received fewer than {threshold} job links
      </div>
    </div>
    <div class="body">
      <p>Hi Team,</p>
      <p>
        This is an automated alert to inform you that <strong>{len(leads_data)} lead(s)</strong> currently have job links below the expected threshold of <strong>{threshold}</strong>.
      </p>

      <div class="alert-box">
        <strong>⚠️ Action Required:</strong> Please review these leads and ensure they are receiving adequate job matches.
      </div>

      <h3 style="color: #111827; margin-top: 24px;">Leads Below Threshold</h3>
      
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>Lead Name</th>
              <th>Email</th>
              <th>APW ID</th>
              <th>Years Exp</th>
              <th>Job Role</th>
              <th>Service Opted</th>
              <th>Job Links</th>
              <th>Threshold</th>
              <th>Shortfall</th>
            </tr>
          </thead>
          <tbody>
            {leads_rows}
          </tbody>
        </table>
      </div>

      <p style="margin-top: 24px; color: #6b7280; font-size: 13px;">
        <strong>Suggested Actions:</strong>
      </p>
      <ul style="color: #6b7280; font-size: 13px;">
        <li>Review lead profiles and scoring criteria</li>
        <li>Check if job sources are providing adequate matches</li>
        <li>Consider adjusting target role parameters</li>
        <li>Verify lead preferences and requirements</li>
      </ul>

      <a href="{support_url}" class="cta-button" target="_blank" rel="noopener noreferrer">
        View Dashboard
      </a>

      <p style="margin-top: 24px; color: #9ca3af; font-size: 12px;">
        This is an automated notification from {app_name}. Generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}.
      </p>
    </div>
    <div class="footer">
      © {datetime.utcnow().year} {app_name}. All rights reserved.
    </div>
  </div>
</body>
</html>
"""
    
    return html

# -----------------------
# NEW ENDPOINT: Check ALL leads below threshold
# -----------------------
@app.post("/check-all-leads-threshold")
def check_all_leads_threshold():
    """
    Check ALL leads in the database and find those below threshold.
    Send ONE email with a summary of all leads below threshold.
    """
    # Query database for all leads below threshold
    leads_below_threshold = get_all_leads_below_threshold(JOB_LINKS_THRESHOLD)
    
    if not leads_below_threshold:
        return {
            "success": True,
            "message": "All leads are above threshold!",
            "leads_count": 0,
            "threshold": JOB_LINKS_THRESHOLD,
            "email_sent": False
        }
    
    # Build email with all leads information
    subject = f"{APP_NAME} Alert: {len(leads_below_threshold)} Lead(s) Below Job Links Threshold"
    
    html = build_multi_lead_email_template(
        app_name=APP_NAME,
        leads_data=leads_below_threshold,
        threshold=JOB_LINKS_THRESHOLD,
    )
    
    # Send email with all leads information (with CC)
    send_mail_via_graph(
        to=TEST_EMAIL_RECIPIENT,
        subject=subject,
        html=html,
        cc=CC_EMAIL_RECIPIENTS,
    )
    
    print(f"✅ Batch threshold alert email sent to {TEST_EMAIL_RECIPIENT}")
    print(f"   Total leads below threshold: {len(leads_below_threshold)}")
    print(f"   Threshold: {JOB_LINKS_THRESHOLD}")
    for lead in leads_below_threshold[:5]:  # Print first 5
        print(f"   - {lead['lead_name']} ({lead['job_count_today']} job links)")
    if len(leads_below_threshold) > 5:
        print(f"   ... and {len(leads_below_threshold) - 5} more")
    
    return {
        "success": True,
        "message": f"Found {len(leads_below_threshold)} lead(s) below threshold - batch notification email sent",
        "leads_count": len(leads_below_threshold),
        "threshold": JOB_LINKS_THRESHOLD,
        "leads": leads_below_threshold,
        "email_sent": True,
        "email_sent_to": TEST_EMAIL_RECIPIENT
    }


# Health check endpoint
@app.get("/")
def health_check():
    return {"status": "ok", "service": "Job Links Threshold Notification API"}


# -----------------------
# Main execution when run as script (for cron jobs)
# -----------------------
if __name__ == "__main__":
    print("🚀 Starting email notification check...")
    try:
        result = check_all_leads_threshold()
        print(f"✅ Execution completed successfully!")
        print(f"Result: {result}")
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
