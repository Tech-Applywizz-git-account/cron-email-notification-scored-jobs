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
TEST_EMAIL_RECIPIENT = "bhanutejathouti@gmail.com"

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
# Query Task Status Summary for Today
# -----------------------
def get_task_status_summary():
    """
    Query tasks created today and get status breakdown for tasks with score >= 75.
    Groups by lead and CA (ops person).
    
    Returns list of dicts with lead info, CA info, and task status counts.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = """
                SELECT
                    l."apwId" AS apw_id,
                    l.email AS lead_email,
                    op.name AS ca_name,
                    op.email AS ca_email,

                    COUNT(*) FILTER (
                        WHERE sj.score >= 75
                          AND LOWER(t.status) = 'completed'
                    ) AS completed_75_plus,

                    COUNT(*) FILTER (
                        WHERE sj.score >= 75
                          AND LOWER(t.status) = 'pending'
                    ) AS pending_75_plus,

                    COUNT(*) FILTER (
                        WHERE sj.score >= 75
                          AND LOWER(t.status) = 'in_progress'
                    ) AS in_progress_75_plus,

                    COUNT(*) FILTER (
                        WHERE sj.score >= 75
                          AND LOWER(t.status) = 'not_relevant'
                    ) AS not_relevant_75_plus,

                    COUNT(*) FILTER (
                        WHERE sj.score >= 75
                          AND LOWER(t.status) = 'job_not_found'
                    ) AS job_not_found_75_plus,

                    COUNT(*) FILTER (
                        WHERE sj.score >= 75
                          AND LOWER(t.status) = 'already_applied'
                    ) AS already_applied_75_plus

                FROM karmafy_task t
                JOIN karmafy_scoredjob sj
                    ON sj.id = t."scored_jobId"
                JOIN karmafy_lead l
                    ON l.id = t."leadId"
                LEFT JOIN public."karmafy_opsPerson" op
                    ON op.user_id = t."ops_personId"

                WHERE t."createdAt" >= TIMESTAMPTZ '2025-12-23 00:00:00+00'
  AND t."createdAt" <  TIMESTAMPTZ '2025-12-24 00:00:00+00'

                GROUP BY
                    l."apwId",
                    l.email,
                    op.name,
                    op.email

                ORDER BY
                    op.name
            """
            cursor.execute(query)
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


def build_task_status_email_template(
    app_name: str,
    tasks_data: list,
    support_url: str = "https://dashboard.apply-wizz.com/",
) -> str:
    """Build the HTML email template with task status breakdown for tasks with score >= 75"""
    
    # Build table rows for all task summaries
    task_rows = ""
    for idx, task in enumerate(tasks_data, 1):
        apw_id = task.get('apw_id', 'N/A') or 'N/A'
        lead_email = task.get('lead_email', 'N/A')
        ca_name = task.get('ca_name', 'N/A') or 'N/A'
        ca_email = task.get('ca_email', 'N/A') or 'N/A'
        
        completed = task.get('completed_75_plus', 0)
        pending = task.get('pending_75_plus', 0)
        in_progress = task.get('in_progress_75_plus', 0)
        not_relevant = task.get('not_relevant_75_plus', 0)
        job_not_found = task.get('job_not_found_75_plus', 0)
        already_applied = task.get('already_applied_75_plus', 0)
        
        total_tasks = completed + pending + in_progress + not_relevant + job_not_found + already_applied
        
        task_rows += f"""
        <tr style="border-bottom: 1px solid #e5e7eb;">
            <td style="padding: 10px 8px; text-align: center; color: #6b7280; font-weight: 600; font-size: 13px;">{idx}</td>
            <td style="padding: 10px 8px; text-align: center; color: #111827; font-weight: 600; font-size: 13px;">{apw_id}</td>
            <td style="padding: 10px 8px; color: #4b5563; font-family: ui-monospace, monospace; font-size: 12px;">{lead_email}</td>
            <td style="padding: 10px 8px; color: #111827; font-weight: 600; font-size: 13px;">{ca_name}</td>
            <td style="padding: 10px 8px; color: #6b7280; font-family: ui-monospace, monospace; font-size: 12px;">{ca_email}</td>
            <td style="padding: 10px 8px; text-align: center; font-weight: 700; color: #059669; font-size: 13px;">{completed}</td>
            <td style="padding: 10px 8px; text-align: center; font-weight: 700; color: #f59e0b; font-size: 13px;">{pending}</td>
            <td style="padding: 10px 8px; text-align: center; font-weight: 700; color: #3b82f6; font-size: 13px;">{in_progress}</td>
            <td style="padding: 10px 8px; text-align: center; font-weight: 700; color: #6b7280; font-size: 13px;">{not_relevant}</td>
            <td style="padding: 10px 8px; text-align: center; font-weight: 700; color: #dc2626; font-size: 13px;">{job_not_found}</td>
            <td style="padding: 10px 8px; text-align: center; font-weight: 700; color: #8b5cf6; font-size: 13px;">{already_applied}</td>
            <td style="padding: 10px 8px; text-align: center; font-weight: 700; color: #111827; font-size: 13px; background-color: #f3f4f6;">{total_tasks}</td>
        </tr>
        """
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charSet="UTF-8" />
  <title>{app_name} – Daily Task Status Report (Score ≥ 75)</title>
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
      max-width: 1400px;
      margin: 32px auto;
      background: #ffffff;
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 18px 45px rgba(15, 23, 42, 0.12);
    }}
    .header {{
      background: linear-gradient(135deg, #3b82f6, #8b5cf6);
      padding: 24px 32px;
      color: white;
      text-align: left;
    }}
    .title {{
      background-color: #3b82f6;
      margin-top: 10px;
      font-size: 24px;
      font-weight: 700;
      color: white;
    }}
    .sub {{
      background-color: #3b82f6;
      margin-top: 6px;
      font-size: 14px;
      opacity: 0.9;
      color: white;
    }}
    .body {{
      padding: 24px 32px 32px;
      font-size: 14px;
      line-height: 1.6;
    }}
    .info-box {{
      background-color: #eff6ff;
      border-left: 4px solid #3b82f6;
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
      min-width: 1300px;
      border-collapse: collapse;
      background: white;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      overflow: hidden;
    }}
    th {{
      background: #f9fafb;
      padding: 10px 8px;
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
      padding: 10px 8px;
      font-size: 13px;
      border-bottom: 1px solid #f3f4f6;
    }}
    th:nth-child(1), th:nth-child(2), th:nth-child(6), th:nth-child(7), th:nth-child(8), 
    th:nth-child(9), th:nth-child(10), th:nth-child(11), th:nth-child(12) {{
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
      background: linear-gradient(135deg, #3b82f6, #8b5cf6);
      color: white !important;
      text-decoration: none;
      border-radius: 999px;
      font-weight: 600;
      font-size: 14px;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      margin: 20px 0;
      padding: 16px;
      background: #f9fafb;
      border-radius: 8px;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
    }}
    .legend-color {{
      width: 16px;
      height: 16px;
      border-radius: 4px;
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="title">📊 Daily Task Status Report (Score ≥ 75)</div>
      <div class="sub">
        Task status breakdown for {len(tasks_data)} lead-CA combination(s) - {datetime.utcnow().strftime('%Y-%m-%d')}
      </div>
    </div>
    <div class="body">
      <p>Hi Team,</p>
      <p>
        This is your daily task status report showing the breakdown of all tasks created today with a score of <strong>75 or higher</strong>, grouped by lead and assigned CA (Customer Advocate).
      </p>

      <div class="info-box">
        <strong>ℹ️ Report Details:</strong> This report includes only tasks with scores ≥ 75 created on {datetime.utcnow().strftime('%Y-%m-%d')} (UTC).
      </div>

      <div class="legend">
        <div class="legend-item">
          <div class="legend-color" style="background-color: #059669;"></div>
          <span><strong>Completed:</strong> Tasks successfully completed</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background-color: #f59e0b;"></div>
          <span><strong>Pending:</strong> Tasks awaiting action</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background-color: #3b82f6;"></div>
          <span><strong>In Progress:</strong> Tasks currently being worked on</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background-color: #6b7280;"></div>
          <span><strong>Not Relevant:</strong> Tasks marked as not relevant</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background-color: #dc2626;"></div>
          <span><strong>Job Not Found:</strong> Tasks where job posting was not found</span>
        </div>
        <div class="legend-item">
          <div class="legend-color" style="background-color: #8b5cf6;"></div>
          <span><strong>Already Applied:</strong> Jobs already applied to</span>
        </div>
      </div>

      <h3 style="color: #111827; margin-top: 24px;">Task Status Breakdown by Lead & CA</h3>
      
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>APW ID</th>
              <th>Lead Email</th>
              <th>CA Name</th>
              <th>CA Email</th>
              <th style="background-color: #ecfdf5;">Completed</th>
              <th style="background-color: #fffbeb;">Pending</th>
              <th style="background-color: #eff6ff;">In Progress</th>
              <th style="background-color: #f9fafb;">Not Relevant</th>
              <th style="background-color: #fef2f2;">Job Not Found</th>
              <th style="background-color: #f5f3ff;">Already Applied</th>
              <th style="background-color: #f3f4f6;">Total</th>
            </tr>
          </thead>
          <tbody>
            {task_rows}
          </tbody>
        </table>
      </div>

      <p style="margin-top: 24px; color: #6b7280; font-size: 13px;">
        <strong>Key Insights:</strong>
      </p>
      <ul style="color: #6b7280; font-size: 13px;">
        <li>Review leads with high "Not Relevant" or "Job Not Found" counts</li>
        <li>Monitor "Pending" tasks to ensure timely completion</li>
        <li>Check "Already Applied" tasks to avoid duplicate applications</li>
        <li>Celebrate leads with high "Completed" task counts</li>
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
# NEW ENDPOINT: Send daily task status report
# -----------------------
@app.post("/send-task-status-report")
def send_task_status_report():
    """
    Send daily task status report for tasks with score >= 75.
    Shows breakdown by status (completed, pending, in_progress, etc.) grouped by lead and CA.
    """
    # Query database for task status summary
    task_summary = get_task_status_summary()
    
    if not task_summary:
        return {
            "success": True,
            "message": "No tasks found for today with score >= 75",
            "task_count": 0,
            "email_sent": False
        }
    
    # Build email with task status breakdown
    subject = f"{APP_NAME} - Daily Task Status Report (Score ≥ 75) - {datetime.utcnow().strftime('%Y-%m-%d')}"
    
    html = build_task_status_email_template(
        app_name=APP_NAME,
        tasks_data=task_summary,
    )
    
    # Send email with task status information (with CC)
    send_mail_via_graph(
        to=TEST_EMAIL_RECIPIENT,
        subject=subject,
        html=html,
        cc=CC_EMAIL_RECIPIENTS,
    )
    
    print(f"✅ Daily task status report sent to {TEST_EMAIL_RECIPIENT}")
    print(f"   Total lead-CA combinations: {len(task_summary)}")
    for task in task_summary[:5]:  # Print first 5
        total = sum([
            task.get('completed_75_plus', 0),
            task.get('pending_75_plus', 0),
            task.get('in_progress_75_plus', 0),
            task.get('not_relevant_75_plus', 0),
            task.get('job_not_found_75_plus', 0),
            task.get('already_applied_75_plus', 0)
        ])
        print(f"   - Lead {task['apw_id']} / CA {task['ca_name']}: {total} tasks")
    if len(task_summary) > 5:
        print(f"   ... and {len(task_summary) - 5} more")
    
    return {
        "success": True,
        "message": f"Daily task status report sent for {len(task_summary)} lead-CA combination(s)",
        "task_combinations_count": len(task_summary),
        "tasks_data": task_summary,
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
    print("🚀 Starting daily task status report...")
    try:
        result = send_task_status_report()
        print(f"✅ Execution completed successfully!")
        print(f"Result: {result}")
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
