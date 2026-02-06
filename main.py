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
SHEET_ART_NAME = "Article"

# Blacklist Domains
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
        raise Exception("Không tìm thấy biến môi trường GCP_SA_KEY.")
    
    creds_dict = json.loads(creds_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

def search_serper(query, api_key, num_results=10):
    """Hàm search core"""
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
        print(f"Lỗi Serper API khi search '{query}': {e}")
        return []

def process_and_save(keywords, target_sheet_obj, api_key, flow_name):
    """
    Logic xử lý: Search -> Check trùng với data cũ -> Ghi URL mới
    """
    print(f"\n🚀 BẮT ĐẦU {flow_name}...")
    print(f"-> Số lượng keyword cần chạy: {len(keywords)}")
    
    if not keywords:
        print("-> Không có keyword nào. Skip.")
        return

    # --- BƯỚC 1: LẤY DỮ LIỆU CŨ ĐỂ CHECK TRÙNG ---
    try:
        # Lấy toàn bộ cột A hiện có trong sheet đích
        existing_urls_list = target_sheet_obj.col_values(1)
        # Đưa vào SET để check cho nhanh (O(1))
        existing_urls_set = set(existing_urls_list)
        print(f"-> Đã load {len(existing_urls_set)} URL cũ để đối chiếu.")
    except Exception as e:
        print(f"Warning: Không đọc được dữ liệu cũ (có thể sheet rỗng): {e}")
        existing_urls_set = set()

    data_to_write = []
    
    # --- BƯỚC 2: QUÉT VÀ LỌC ---
    for kw in keywords:
        print(f"   Searching: {kw}")
        urls = search_serper(kw, api_key)
        
        new_urls_count_for_kw = 0
        
        for url in urls:
            # Check trùng: Nếu URL chưa từng có trong Set thì mới lấy
            if url not in existing_urls_set:
                data_to_write.append([url])
                
                # Quan trọng: Thêm ngay vào Set để tránh trùng lặp 
                # ngay trong chính lần chạy này (nếu 2 kw ra cùng 1 url)
                existing_urls_set.add(url)
                new_urls_count_for_kw += 1
        
        # In ra log nhẹ để biết keyword này kiếm được bao nhiêu cái mới
        if new_urls_count_for_kw > 0:
            print(f"     -> Thêm được {new_urls_count_for_kw} URL mới.")
            
        time.sleep(0.5) 

    # --- BƯỚC 3: GHI DỮ LIỆU ---
    if data_to_write:
        print(f"-> Đang ghi tổng cộng {len(data_to_write)} URL MỚI TINH vào sheet...")
        target_sheet_obj.append_rows(data_to_write)
        print(f"✅ {flow_name}: HOÀN THÀNH.")
    else:
        print(f"⚠️ {flow_name}: Không có URL nào mới (toàn bộ đã trùng hoặc không tìm thấy).")

def main():
    print("--- STARTING DUAL FLOW JOB (WITH DEDUPLICATION) ---")
    
    try:
        # 1. Init Connections
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        
        kw_sheet = sh.worksheet(SHEET_KW_NAME)
        pub_sheet = sh.worksheet(SHEET_PUB_NAME)
        art_sheet = sh.worksheet(SHEET_ART_NAME) 
        
        serper_api_key = os.environ.get("SERPER_API_KEY")
        if not serper_api_key:
             raise Exception("Thiếu SERPER_API_KEY")

        # 2. CHUẨN BỊ DỮ LIỆU ĐẦU VÀO
        
        # --- LUỒNG 1: Keyword cột A (Article) ---
        raw_col_a = kw_sheet.col_values(1)[1:] 
        keywords_group_a = [k for k in raw_col_a if k.strip()]

        # --- LUỒNG 2: Keyword cột B (Publisher) ---
        raw_col_b = kw_sheet.col_values(2)[1:] 
        keywords_group_b = [k for k in raw_col_b if k.strip()]

        # 3. THỰC THI CÁC LUỒNG
        
        # Chạy luồng cho Article
        process_and_save(keywords_group_a, art_sheet, serper_api_key, flow_name="FLOW 1 [Article]")

        # Chạy luồng cho Publisher
        process_and_save(keywords_group_b, pub_sheet, serper_api_key, flow_name="FLOW 2 [Publisher]")

    except Exception as e:
        print(f"\n❌ LỖI HỆ THỐNG: {e}")
        sys.exit(1)

    print("\n--- JOB FINISHED SUCCESSFULLY ---")

if __name__ == "__main__":
    main()
