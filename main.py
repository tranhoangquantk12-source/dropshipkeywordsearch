import os
import json
import gspread
import requests
import time
import sys
from oauth2client.service_account import ServiceAccountCredentials

# --- CẤU HÌNH ---
SPREADSHEET_ID = "1uvjEg0XtG_Q8jNjPVQ6FP_9cIvqxyur-I-PHNggUy5s"
SHEET_KW_NAME = "kw"
SHEET_PUB_NAME = "Publisher"

EXCLUDE_DOMAINS = [
    "youtube.com", "shopify.com", "autods.com", "omnisend.com", 
    "reddit.com", "quora.com", "coursera.org", "classcentral.com", 
    "trueprofit.io", "beprofit.co", "facebook.com", "instagram.com", 
    "tiktok.com", "threads.com", "x.com", "cursa.app", 
    "coursesity.com", "scribd.com", "alison.com", "udemy.com"
]

def get_google_sheet_client():
    creds_json = os.environ.get("GCP_SA_KEY")
    if not creds_json:
        raise Exception("Không tìm thấy biến môi trường GCP_SA_KEY. Kiểm tra lại Github Secrets!")
    
    creds_dict = json.loads(creds_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

def search_serper(query, api_key, num_results=10):
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query, "num": 30})
    headers = {'X-API-KEY': api_key, 'Content-Type': 'application/json'}

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        
        if "organic" not in data:
            return []

        clean_results = []
        count = 0
        for item in data["organic"]:
            link = item.get("link")
            if not link: continue
            
            domain = link.split("//")[-1].split("/")[0].lower()
            is_blocked = False
            for blocked in EXCLUDE_DOMAINS:
                if blocked in domain:
                    is_blocked = True
                    break
            
            if not is_blocked:
                clean_results.append(link)
                count += 1
            if count == num_results:
                break     
        return clean_results

    except Exception as e:
        print(f"Lỗi Serper API: {e}")
        return []

def main():
    print("--- BẮT ĐẦU JOB (CHỈ LẤY URL) ---")
    
    try:
        # 1. Setup
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        kw_sheet = sh.worksheet(SHEET_KW_NAME)
        pub_sheet = sh.worksheet(SHEET_PUB_NAME)
        
        serper_api_key = os.environ.get("SERPER_API_KEY")
        if not serper_api_key:
             raise Exception("Thiếu SERPER_API_KEY")

        # 2. Đọc Keywords
        col_values = kw_sheet.col_values(2)[1:] 
        keywords = [k for k in col_values if k.strip()]
        
        if not keywords:
            print("Không có keyword nào để chạy.")
            return

        final_data = []

        # 3. Chạy loop
        for kw in keywords:
            print(f"Searching: {kw}")
            urls = search_serper(kw, serper_api_key)
            
            for url in urls:
                # --- THAY ĐỔI QUAN TRỌNG Ở ĐÂY ---
                # Chỉ append đúng 1 phần tử là URL vào list
                # Cấu trúc list lồng nhau: [[url1], [url2], [url3]...]
                final_data.append([url]) 
                
            time.sleep(0.5)

        # 4. Ghi data vào cột A
        if final_data:
            # append_rows sẽ tự tìm dòng trống tiếp theo để ghi
            # Vì data chỉ có 1 cột, nó sẽ chỉ điền vào cột A
            pub_sheet.append_rows(final_data)
            print(f"Đã ghi {len(final_data)} URL vào cột A.")
        else:
            print("Không có URL nào mới.")

    except Exception as e:
        print(f"\n❌ LỖI: {e}")
        sys.exit(1)

    print("--- KẾT THÚC JOB ---")

if __name__ == "__main__":
    main()
