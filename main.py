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
SHEET_ART_NAME = "Article" # Sheet mới cho flow 2

# Blacklist Domains
EXCLUDE_DOMAINS = [
    "youtube.com", "shopify.com", "autods.com", "omnisend.com", 
    "reddit.com", "quora.com", "coursera.org", "classcentral.com", 
    "trueprofit.io", "beprofit.co", "facebook.com", "instagram.com", 
    "tiktok.com", "threads.com", "x.com", "cursa.app", 
    "coursesity.com", "scribd.com", "alison.com", "udemy.com" , "zendrop.com",
]

def get_google_sheet_client():
    creds_json = os.environ.get("GCP_SA_KEY")
    if not creds_json:
        raise Exception("Không tìm thấy biến môi trường GCP_SA_KEY.")
    
    creds_dict = json.loads(creds_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

def search_serper(query, api_key, num_results=10):
    """Hàm search core: Gọi API và lọc domain rác"""
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": query, "num": 30}) # Lấy dư để lọc
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
        print(f"Lỗi Serper API khi search '{query}': {e}")
        return []

def process_and_save(keywords, target_sheet_obj, api_key, flow_name):
    """
    Hàm xử lý logic chung cho mọi luồng:
    Input: List Keywords -> Search -> Output: Ghi URL vào Target Sheet
    """
    print(f"\n🚀 BẮT ĐẦU {flow_name}...")
    print(f"-> Số lượng keyword cần chạy: {len(keywords)}")
    
    if not keywords:
        print("-> Không có keyword nào. Skip.")
        return

    data_buffer = []
    
    for kw in keywords:
        print(f"   Searching: {kw}")
        urls = search_serper(kw, api_key)
        
        for url in urls:
            # Chỉ lấy URL, mỗi URL 1 dòng, 1 cột
            data_buffer.append([url])
            
        time.sleep(0.5) # Delay nhẹ tránh spam

    if data_buffer:
        print(f"-> Đang ghi {len(data_buffer)} URLs vào sheet...")
        target_sheet_obj.append_rows(data_buffer)
        print(f"✅ {flow_name}: HOÀN THÀNH.")
    else:
        print(f"⚠️ {flow_name}: Không tìm thấy dữ liệu mới nào.")

def main():
    print("--- STARTING DUAL FLOW JOB ---")
    
    try:
        # 1. Init Connections
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        
        # Lấy các sheet cần thiết
        kw_sheet = sh.worksheet(SHEET_KW_NAME)
        pub_sheet = sh.worksheet(SHEET_PUB_NAME)
        art_sheet = sh.worksheet(SHEET_ART_NAME) # Sheet Article
        
        serper_api_key = os.environ.get("SERPER_API_KEY")
        if not serper_api_key:
             raise Exception("Thiếu SERPER_API_KEY")

        # 2. CHUẨN BỊ DỮ LIỆU ĐẦU VÀO
        
        # --- LUỒNG 1: Keyword cột A (Article) ---
        # Lấy cột 1, bỏ header dòng 1
        raw_col_a = kw_sheet.col_values(1)[1:] 
        keywords_group_a = [k for k in raw_col_a if k.strip()]

        # --- LUỒNG 2: Keyword cột B (Publisher) ---
        # Lấy cột 2, bỏ header dòng 1
        raw_col_b = kw_sheet.col_values(2)[1:] 
        keywords_group_b = [k for k in raw_col_b if k.strip()]

        # 3. THỰC THI CÁC LUỒNG
        
        # Chạy luồng cho Article (Input A -> Output Article)
        process_and_save(keywords_group_a, art_sheet, serper_api_key, flow_name="FLOW 1 [Article]")

        # Chạy luồng cho Publisher (Input B -> Output Publisher)
        process_and_save(keywords_group_b, pub_sheet, serper_api_key, flow_name="FLOW 2 [Publisher]")

    except Exception as e:
        print(f"\n❌ LỖI HỆ THỐNG: {e}")
        sys.exit(1)

    print("\n--- JOB FINISHED SUCCESSFULLY ---")

if __name__ == "__main__":
    main()
