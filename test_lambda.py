# test_lambda.py
import json
from lambda_function import lambda_handler

# Simulate a webhook event from Merge API
test_event = {
  "body": json.dumps({
    "hook": {
      "id": "cb1fe0a7-c2a1-4bd6-8cf5-57e70c7f1d53",
      "event": "File.changed",
      "target": "<webhook_url>",
    },
    "linked_account": {
      "id": "f3e22209-b26b-4598-9c16-59eddba6a047",
      "integration": "Google Drive",
      "integration_slug": "google-drive",
      "category": "filestorage",
      "end_user_origin_id": "",
      "end_user_organization_name": "Test",
      "end_user_email_address": "test@merge.dev",
      "status": "COMPLETE",
      "webhook_listener_url": "https://api.merge.dev/api/integrations/webhook-listener/IDS",
      "is_duplicate": None,
      "account_type": "PRODUCTION"
    },
    "data": {
      "id": "2aefec79-7f0b-4486-a192-19e19b14483f",
      "remote_id": "12",
      "created_at": "2021-09-15T00:00:00Z",
      "modified_at": "2021-11-20T00:00:00Z",
      "name": "merge_file_storage_launch.docx",
      "file_url": "https://drive.google.com/file/d/1234",
      "file_thumbnail_url": "https://drive.google.com/thumbnail?id=1234",
      "size": 256,
      "mime_type": "application/vnd.google-apps.document",
      "description": "Updated file content with new data for redaction purposes.",
      "folder": "8e889422-e086-42dc-b99e-24d732039b0b",
      "permissions": [
        {
          "id": "31ce489c-asdf-68b1-754r-629f799f7123",
          "remote_id": "102895",
          "created_at": "2020-03-31T00:00:00Z",
          "modified_at": "2020-06-20T00:00:00Z",
          "user": "21ce474c-asdf-34a2-754r-629f799f7d12",
          "group": None,
          "type": "USER",
          "roles": [
            "OWNER"
          ],
          "remote_data": None
        },
        {
          "id": "2ea7db93-1ae9-4686-82c9-35c768000736",
          "remote_id": None,
          "created_at": "2020-03-31T00:00:00Z",
          "modified_at": "2020-06-20T00:00:00Z",
          "user": None,
          "group": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
          "type": "GROUP",
          "roles": [
            "READ"
          ],
          "remote_data": None
        }
      ],
      "drive": "google_drive",
      "remote_created_at": "2022-02-02T00:00:00Z",
      "remote_updated_at": "2022-03-01T00:00:00Z"
    }
  })
}

# Context is not used in our function, so we pass None
response = lambda_handler(test_event, None)
print(response)

