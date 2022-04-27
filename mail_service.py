# https://developers.google.com/gmail/api/quickstart/python
# https://developers.google.com/gmail/api/guides/sending

from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from email.mime.text import MIMEText
import base64
import urllib

from google.cloud import storage

bucket_name = "mail-service-cred"
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# https://developers.google.com/workspace/guides/identify-scopes
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.send', 
          'https://www.googleapis.com/auth/gmail.compose']

def init_gmail_service():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'desktop_client_1_OAuth_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    return service


# To avoid the issue where you have to store an updated token,
# use this one to store and retrieve token from GCS buckets.
def init_gmail_service_with_gcs():
    print ('Initiating mail service......')
    blob = bucket.blob('token.json')
    blob.download_to_filename('/tmp/token.json')
    
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('/tmp/token.json'):
        print ('Token.json file found.')
        creds = Credentials.from_authorized_user_file('/tmp/token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'desktop_client_1_OAuth_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open('/tmp/token.json', 'w') as token:
            token.write(creds.to_json())
        # Save the credentials for the next run
        blob = bucket.blob('token.json')
        blob.upload_from_filename('/tmp/token.json')

    service = build('gmail', 'v1', credentials=creds)
    return service


def create_message(sender, to, subject, message_text):
    """Create a message for an email.

    Args:
        sender: Email address of the sender.
        to: Email address of the receiver.
        subject: The subject of the email message.
        message_text: The text of the email message.

    Returns:
        An object containing a base64url encoded email object.
    """
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    #return {'raw': base64.urlsafe_b64encode(message.as_string())}
    return {'raw': base64.urlsafe_b64encode(message.as_string().encode('utf-8')).decode('utf-8')}


def send_message(service, user_id, message):
    """
    Args:
        service: Authorized Gmail API service instance.
        user_id: User's email address. The special value "me"
                can be used to indicate the authenticated user.
        message: Message to be sent.

    Returns:
        Sent Message.
    """
    try:
        message = (service.users().messages().send(userId=user_id, body=message).execute())
        print ('Message Id: %s' % message['id'])
        return message
    except urllib.error.HTTPError as exception:
        print(exception)

