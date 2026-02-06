import os
import json
import gspread
import requests
import time
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# --- CẤU HÌNH ---
SPREADSHEET_ID = "1uvjEg0XtG_Q8jNjPVQ6FP_9cIvqxyur-I-PHNggUy5s"
SHEET_KW_NAME = "kw"
SHEET_PUB_NAME = "Publisher"

# Danh sách domain cần loại bỏ
EXCLUDE_DOMAINS = [
    "youtube.com", "shopify.com", "autods.com", "omnisend.com", 
    "reddit.com", "quora.com", "coursera.org", "classcentral.com", 
    "trueprofit.io", "beprofit.co", "facebook.com", "instagram.com", 
    "tiktok.com", "threads.com", "x.com", "cursa.app", 
    "coursesity.com", "scribd.com", "alison.com", "udemy.com"
]

def get_google_sheet_client():
    """Kết nối Google Sheet"""
    creds_json = os.environ.get("GCP_SA_KEY")
    if not creds_json:
        raise Exception("Không tìm thấy biến môi trường GCP_SA_KEY")
    
    creds_dict = json.loads(creds_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

def search_serper(query, api_key, num_results=10):
    """
    Search Google thông qua Serper.dev
    Lợi thế: Có thể lấy 20-30 kết quả trong 1 request để lọc dần
    """
    url = "https://google.serper.dev/search"
    
    # Xin hẳn 30 kết quả để trừ hao những domain bị blacklist
    payload = json.dumps({
        "q": query,
        "num": 30 
    })
    headers = {
        'X-API-KEY': api_key,
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        data = response.json()
        
        if "organic" not in data:
            print(f"Không tìm thấy kết quả organic cho: {query}")
            return []

        clean_results = []
        count = 0
        
        for item in data["organic"]:
            link = item.get("link")
            if not link: continue
            
            # Xử lý check domain blacklist
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
        print(f"Lỗi khi gọi Serper API: {e}")
        return []

def main():
    print("--- BẮT ĐẦU JOB (SERPER VERSION) ---")
    
    # 1. Setup kết nối Sheet
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        kw_sheet = sh.worksheet(SHEET_KW_NAME)
        pub_sheet = sh.worksheet(SHEET_PUB_NAME)
        
        # Lấy Serper Key từ Github Secret
        serper_api_key = os.environ.get("SERPER_API_KEY")
        if not serper_api_key:
             raise Exception("Thiếu SERPER_API_KEY trong Github Secrets")

    except Exception as e:
        print(f"Lỗi setup ban đầu: {e}")
        return

    # 2. Đọc Keywords
    col_values = kw_sheet.col_values(2)[1:] 
    keywords = [k for k in col_values if k.strip()]
    
    print(f"Tìm thấy {len(keywords)} keywords cần xử lý.")

    final_data_to_write = []
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 3. Loop và Search
    for kw in keywords:
        print(f"Đang xử lý: {kw}...")
        
        # Gọi hàm search mới
        found_urls = search_serper(kw, serper_api_key, num_results=10)
        
        if not found_urls:
            print(f" -> Cảnh báo: Không tìm thấy URL nào sạch cho '{kw}'")

        for idx, url in enumerate(found_urls):
            row = [current_time, kw, url, idx + 1]
            final_data_to_write.append(row)
        
        # Ngủ 0.5s để server đỡ bị spam
        time.sleep(0.5)

    # 4. Ghi vào sheet
    if final_data_to_write:
        print(f"Đang ghi {len(final_data_to_write)} dòng vào sheet Publisher...")
        pub_sheet.append_rows(final_data_to_write)
        print("Hoàn thành ghi dữ liệu.")
    else:
        print("Không có dữ liệu mới để ghi.")

    print("--- KẾT THÚC JOB ---")

if __name__ == "__main__":
    main()
