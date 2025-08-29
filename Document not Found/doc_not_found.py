import os
import sys
import requests

# === Log file ===
log_file = "DOC_NOT_FOUND.txt"
sys.stdout = open(log_file, "w", encoding="utf-8")
sys.stderr = sys.stdout

# === API Config ===
API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"
FILES_DIR = r"C:\Users\dghvmuser05\Desktop\DGH MIGRATION\Scripts\Document not Found"
file_path = os.path.join(FILES_DIR, "document_not_found.pdf")

# === Prepare request ===
try:
    with open(file_path, "rb") as f:
        files = {'files': f}
        data = {
            'regime': 'DOCUMENT NOT FOUND',
            'block': 'DOCUMENT NOT FOUND',
            'module': 'DOCUMENT NOT FOUND',
            'process': 'DOCUMENT NOT FOUND',
            'financialYear': '2025-2026',
            'referenceNumber': 'DOCUMENT NOT FOUND',
            'label': 'DOCUMENT NOT FOUND'
        }

        print("Uploading document_not_found.pdf ...")

        response = requests.post(API_URL, files=files, data=data)

        print("=== API RESPONSE ===")
        print("Status Code:", response.status_code)
        try:
            print(response.json())  # pretty JSON response
        except Exception:
            print(response.text)  # fallback if not JSON

except Exception as e:
    print("Error while uploading:", str(e))
