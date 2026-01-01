
import urllib.request
import urllib.error
import time
import sys
import json

PRIMARY_URL = "http://127.0.0.1:8090"
REPLICA_URL = "http://127.0.0.1:8091"
ADMIN_EMAIL = "test@example.com"
ADMIN_PASS = "password123456"

def request(method, url, data=None, headers=None, timeout=5):
    if headers is None:
        headers = {}
    
    if data:
        data = json.dumps(data).encode('utf-8')
        headers["Content-Type"] = "application/json"
    
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            if response.status >= 200 and response.status < 300:
                body = response.read().decode('utf-8')
                try:
                    j = json.loads(body)
                except:
                    j = {}
                return {"status": response.status, "json": j}
            return {"status": response.status}
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        try:
            j = json.loads(body)
        except:
            j = {}
        return {"status": e.code, "text": body, "json": j}
    except Exception as e:
        return {"status": -1, "error": str(e)}

def wait_for_health(url, name, timeout=30):
    print(f"Waiting for {name} ({url})...")
    start = time.time()
    while time.time() - start < timeout:
        res = request("GET", f"{url}/api/health", timeout=1)
        if res["status"] == 200:
            print(f"{name} is up.")
            return True
        time.sleep(1)
    print(f"{name} failed to start.")
    return False

def login_superuser(url):
    print(f"Logging in as superuser on {url}...")
    res = request("POST", f"{url}/api/collections/_superusers/auth-with-password", data={
        "identity": ADMIN_EMAIL,
        "password": ADMIN_PASS
    })
    
    if res["status"] == 200:
        print("Logged in.")
        return res["json"]["token"]
    
    print(f"Failed to login: {res}")
    return None

def verify():
    # 1. Wait for services
    if not wait_for_health(PRIMARY_URL, "Primary"): return False
    if not wait_for_health(REPLICA_URL, "Replica"): return False

    # 2. Login as Superuser (Created via CLI) on Primary
    token = login_superuser(PRIMARY_URL)
    if not token: 
        print("Use 'docker-compose exec primary-pocketbase /usr/local/bin/pocketbase superuser upsert test@example.com password123456' to create user first.")
        return False

    headers = {"Authorization": token}

    # 3. Create Collection (Public)
    print("Creating public collection 'notes'...")
    collection_data = {
        "name": "notes",
        "type": "base",
        "schema": [
            {"name": "title", "type": "text", "required": True},
            {"name": "content", "type": "text"}
        ],
        "listRule": "", # Public read
        "viewRule": ""  # Public read
    }
    res = request("POST", f"{PRIMARY_URL}/api/collections", data=collection_data, headers=headers)
    
    if res["status"] not in [200, 400]:
        print(f"Failed to create collection: {res}")
        return False
    
    if res["status"] == 400 and "name" in str(res.get("text", "")):
         print("Collection 'notes' likely exists. Ensuring it is public...")
         # Fetch collection ID to update? Or just assume it works if we start clean.
         # For simplicity, if it exists, we assume previous runs set it up or we might fail if it's private.
         # Better: Get the collection and update it.
         col_res = request("GET", f"{PRIMARY_URL}/api/collections/notes", headers=headers)
         if col_res["status"] == 200:
             col_id = col_res["json"]["id"]
             request("PATCH", f"{PRIMARY_URL}/api/collections/{col_id}", data={"listRule": "", "viewRule": ""}, headers=headers)

    # 4. Create Record
    print("Creating record...")
    record_data = {"title": "Hello Replication", "content": "This is a test note."}
    res = request("POST", f"{PRIMARY_URL}/api/collections/notes/records", data=record_data, headers=headers)
    
    if res["status"] != 200:
        print(f"Failed to create record: {res}")
        return False
    
    record_id = res["json"]["id"]
    print(f"Record created: {record_id}")

    # 5. Verify on Replica (Read-Only)
    print("Verifying on Replica (allowing 60s lag)...")
    
    # Do NOT login to Replica (avoids writing to WAL)
    start = time.time()
    while time.time() - start < 60:
        res = request("GET", f"{REPLICA_URL}/api/collections/notes/records/{record_id}")
        if res["status"] == 200:
            data = res["json"]
            if data["title"] == "Hello Replication":
                print("SUCCESS: Record found in Replica!")
                return True
        elif res["status"] == 404:
            print("Record not found yet (404)...")
        else:
            print(f"Replica returned {res['status']}")
            
        time.sleep(1)

    print("FAILURE: Record never appeared in Replica.")
    return False

if __name__ == "__main__":
    if verify():
        sys.exit(0)
    else:
        sys.exit(1)
