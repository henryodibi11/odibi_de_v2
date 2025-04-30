import requests

def send_email_using_logic_app(to: str, subject: str, body: str, logic_app_url: str):
    """
    Sends an email by making an HTTP POST request to a specified Logic App endpoint.

    This function constructs a JSON payload with the recipient's email address, subject,
    and body of the email, and sends it to the provided Logic App URL. It handles the response
    by checking the status code to determine if the email was sent successfully.

    Args:
        to (str): The email address of the recipient.
        subject (str): The subject line of the email.
        body (str): The main content of the email, which can be in HTML or plain text format.
        logic_app_url (str): The URL of the Logic App HTTP endpoint that processes the email sending request.

    Returns:
        None: This function does not return any value but prints the status of the email sending operation.

    Raises:
        requests.exceptions.RequestException: An error occurred during the HTTP request to the Logic App endpoint.

    Example:
        send_email_using_logic_app(
            to="example@example.com",
            subject="Hello World",
            body="<h1>This is a test email</h1>",
            logic_app_url="https://prod-00.westus.logic.azure.com/workflows/..."
        )
    """
    payload = {
        'to': to,
        'subject': subject,
        'body': body
    }
    response = requests.post(logic_app_url, json=payload, headers={'Content-Type': 'application/json'})
    if response.status_code not in [200, 202]:
        print(f"[send_email] Failed to send email. Status: {response.status_code}")
    else:
        print("[send_email] Email sent successfully.")
