import os
import json
import psycopg2
import openai
from merge import Merge
from pgvector.psycopg2 import register_vector

# Set OpenAI API key from environment variable.
openai.api_key = os.environ["OPENAI_API_KEY"]

def upsert_merge_file(cursor, data):
    """
    Upserts the file metadata into the merge_files table.
    """
    upsert_query = """
    INSERT INTO merge_files (
        id, remote_id, created_at, modified_at, name,
        file_url, file_thumbnail_url, size, mime_type, description,
        remote_created_at, remote_updated_at
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (id) DO UPDATE SET
        remote_id = EXCLUDED.remote_id,
        created_at = EXCLUDED.created_at,
        modified_at = EXCLUDED.modified_at,
        name = EXCLUDED.name,
        file_url = EXCLUDED.file_url,
        file_thumbnail_url = EXCLUDED.file_thumbnail_url,
        size = EXCLUDED.size,
        mime_type = EXCLUDED.mime_type,
        description = EXCLUDED.description,
        remote_created_at = EXCLUDED.remote_created_at,
        remote_updated_at = EXCLUDED.remote_updated_at;
    """
    file_values = (
        data.get("id"),
        data.get("remote_id"),
        data.get("created_at"),
        data.get("modified_at"),
        data.get("name"),
        data.get("file_url"),
        data.get("file_thumbnail_url"),
        data.get("size"),
        data.get("mime_type"),
        data.get("description"),
        data.get("remote_created_at"),
        data.get("remote_updated_at")
    )
    cursor.execute(upsert_query, file_values)

def upsert_file_embedding(cursor, file_id, section, text, embedding):
    """
    Upserts an embedding record into the file_embeddings table.
    If a record for the given file_id and section already exists, update its content.
    """
    upsert_query = """
    INSERT INTO file_embeddings (file_id, section, text_content, embedding)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (file_id, section) DO UPDATE SET
        text_content = EXCLUDED.text_content,
        embedding = EXCLUDED.embedding;
    """
    cursor.execute(upsert_query, (file_id, section, text, embedding))

def lambda_handler(event, context):
    """
    AWS Lambda handler to process a webhook from the Merge API.
    """
    # 1. Parse the incoming webhook JSON payload.
    try:
        body = json.loads(event.get("body", "{}"))
    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON payload", "details": str(e)})
        }

    data = body.get("data", {})
    file_id = data.get("id")
    if not file_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "No 'id' field provided in the payload."})
        }

    # 2. Create a Merge client and download the file.
    client = Merge(
        api_key=os.environ["MERGE_API_KEY"],
        account_token=os.environ["MERGE_ACCOUNT_TOKEN"]
    )
    response = client.filestorage.files.download_retrieve(
        id=file_id,
        mime_type="txt"
    )

    text = ""
    for chunk in response:
        text += chunk.decode("utf-8")

    # 3. Create an embedding for the text using the OpenAI Embeddings API.
    try:
        openai_client = openai.Client()
        response = openai_client.embeddings.create(
            input=text,
            model="text-embedding-3-small"
        )
        embedding = response.data[0].embedding
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Failed to create embedding for the text", "details": str(e)})
        }

    # 4. Connect to the PostgreSQL RDS instance.
    try:
        connection = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ.get("DB_PORT", 5432),
            database=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"]
        )
        register_vector(connection)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Database connection failed", "details": str(e)})
        }

    # 5. Upsert the file record and insert the embedding.
    try:
        with connection.cursor() as cursor:
            upsert_merge_file(cursor, data)
            upsert_file_embedding(cursor, file_id, "content", text, embedding)
        connection.commit()
    except Exception as e:
        connection.rollback()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Database operation failed", "details": str(e)})
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "File data and embedding successfully upserted into the database."})
    }
