"""
email_utils.py — Henry Chinedu Odibi 🇳🇬 Edition v6
-----------------------------------------------------
Universal HTML email builder for ingestion and transformation pipelines.
Rooted in Nigerian-Igbo symbolism and first-principles philosophy.
"""

from datetime import datetime
from typing import List
import re


def build_ingestion_email_html(
    logs: List[str],
    flag_url: str = "https://flagcdn.com/w20/ng.png"
) -> str:
    """Build a Nigerian-themed HTML email for pipeline success or failure."""

    # --------------------------------------------------------------
    # Helper functions
    # --------------------------------------------------------------
    def extract_between(text, start, end):
        try:
            return text.split(start, 1)[1].split(end, 1)[0].strip()
        except Exception:
            return None

    def clean_error_message(raw: str) -> str:
        if not raw:
            return "No error details available."
        lines = [l for l in raw.splitlines() if not l.strip().startswith("at ")]
        keywords = ("Exception", "FAILED", "Error", "Caused by", "WorkflowException")
        relevant = [l.strip() for l in lines if any(k in l for k in keywords)]
        if not relevant:
            relevant = [raw.strip()]
        msg = " | ".join(relevant[:6])
        msg = re.sub(r"\s+", " ", msg)
        msg = msg.replace("com.databricks.", "").replace("java.base/", "")
        return msg[:1500] or raw

    def extract_run_url(logs: List[str]) -> str | None:
        for line in logs:
            m = re.search(r"https:\/\/[\w\-\.]+\.azuredatabricks\.net\/\S+", line)
            if m:
                return m.group(0).strip()
        return None

    def extract_story_path(logs: List[str]) -> str | None:
        for line in logs:
            if "/dbfs/FileStore/" in line and line.endswith("_story.html"):
                return line.strip()
        return None

    # --------------------------------------------------------------
    # Metadata + context detection
    # --------------------------------------------------------------
    metadata_line = next((l for l in logs if "project=" in l and "table=" in l), "")
    project = extract_between(metadata_line, "project=", " -") or "DefaultProject"
    table = extract_between(metadata_line, "table=", " ") or "DefaultTable"

    error_line = ""
    for line in logs:
        if "Exception" in line or "FAILED" in line or "Error" in line:
            error_line = line
            break

    is_failure = bool(error_line)
    summary = clean_error_message(error_line if is_failure else "Pipeline succeeded.")
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    run_url = extract_run_url(logs)
    story_path = extract_story_path(logs)

    # --------------------------------------------------------------
    # Cultural + visual context
    # --------------------------------------------------------------
    if is_failure:
        header_icon = "🦅"   # Ugo – eagle: foresight & resilience
        header_color = "#d63031"  # deep red
        header_text = "Pipeline Failure ❌"
    else:
        header_icon = "🌰"   # Oji – kola nut: unity & success
        header_color = "#046307"  # forest green
        header_text = "Pipeline Success ✅"

    # --------------------------------------------------------------
    # Action buttons
    # --------------------------------------------------------------
    buttons_html = ""
    if run_url:
        buttons_html += f"""
        <a href="{run_url}" target="_blank"
           style="background-color:#046307;color:white;padding:10px 18px;
                  text-decoration:none;border-radius:6px;font-weight:bold;">
           🔍 View in Databricks
        </a>"""
    if story_path:
        buttons_html += f"""
        <a href="{story_path}" target="_blank"
           style="background-color:#b8860b;color:white;padding:10px 18px;
                  text-decoration:none;border-radius:6px;font-weight:bold;
                  margin-left:10px;">
           📘 View Transformation Story
        </a>"""
    if not buttons_html:
        buttons_html = "<p style='color:#888;'>No direct links available for this run.</p>"

    # --------------------------------------------------------------
    # Final HTML body
    # --------------------------------------------------------------
    return f"""
    <html>
    <body style="font-family:'Segoe UI',Roboto,Arial,sans-serif;
                 background-color:#1a1a1a;color:#f5f5f5;padding:25px;">
        <div style="max-width:740px;margin:auto;background-color:#121212;
                    border-radius:10px;padding:25px;border:1px solid #2e2e2e;">
            <h2 style="color:{header_color};
                       border-bottom:2px solid {header_color};
                       padding-bottom:10px;">
                {header_icon}&nbsp;{header_text}
            </h2>

            <table style="width:100%;margin-top:10px;margin-bottom:10px;">
                <tr><td><b>Project:</b></td><td>{project}</td></tr>
                <tr><td><b>Table:</b></td><td>{table}</td></tr>
                <tr><td><b>Timestamp:</b></td><td>{timestamp}</td></tr>
            </table>

            <p style="margin-top:10px;"><b>Summary:</b><br>
               <span style="color:#ccc;">{summary}</span></p>

            <div style="margin:30px 0 20px 0;text-align:center;">{buttons_html}</div>

            <hr style="border:1px solid #333;margin:25px 0;">

            <div style="text-align:center;line-height:1.5;">
                <i style="color:#ccc;font-family:Georgia,serif;">
                    “Even in failure, truth speaks — if you listen deeply enough.”
                </i><br>
                <b style="color:#046307;">
                    — Henry Chinedu Odibi 
                    <img src="{flag_url}" alt="🇳🇬"
                         style="height:14px;vertical-align:middle;border:none;">
                </b><br>
                <span style="color:#888;font-size:0.85em;">
                    Rooted in truth, refined through transformation.
                </span>
            </div>
        </div>
    </body>
    </html>
    """
