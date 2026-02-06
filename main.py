import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from datetime import datetime
import time

# --- CẤU HÌNH ---
SPREADSHEET_ID = "1uvjEg0XtG_Q8jNjPVQ6FP_9cIvqxyur-I-PHNggUy5s"
SHEET_KW_NAME = "kw"
SHEET_PUB_NAME = "Publisher"

# Danh sách domain cần loại bỏ (Blacklist)
EXCLUDE_DOMAINS = [
    "youtube.com", "shopify.com", "autods.com", "omnisend.com", 
    "reddit.com", "quora.com", "coursera.org", "classcentral.com", 
    "trueprofit.io", "beprofit.co", "facebook.com", "instagram.com", 
    "tiktok.com", "threads.com", "x.com", "cursa.app", 
    "coursesity.com", "scribd.com", "alison.com", "udemy.com"
]

def get_google_sheet_client():
    """Kết nối tới Google Sheet dùng Service Account từ biến môi trường"""
    # Lấy nội dung file JSON từ Github Secret
    creds_json = os.environ.get("GCP_SA_KEY")
    if not creds_json:
        raise Exception("Không tìm thấy biến môi trường GCP_SA_KEY")
    
    creds_dict = json.loads(creds_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

def google_search(query, api_key, cse_id, num_results=10):
    """Hàm search Google và lọc kết quả"""
    service = build("customsearch", "v1", developerKey=api_key)
    results = []
    start_index = 1
    
    # Google API chỉ trả về tối đa 10 kết quả mỗi lần gọi. 
    # Nếu muốn lọc sạch domain rác và vẫn đủ 10, ta có thể cần gọi 2-3 lần (phân trang).
    # Tuy nhiên để tiết kiệm quota, code này sẽ gọi tối đa 20 kết quả thô để lọc.
    
    try:
        # Gọi search lấy 10 kết quả đầu
        res = service.cse().list(q=query, cx=cse_id, num=10, start=1).execute()
        items = res.get("items", [])
        
        # Gọi thêm page 2 (lấy thêm 10 nữa để phòng hờ lọc hết)
        res_2 = service.cse().list(q=query, cx=cse_id, num=10, start=11).execute()
        items.extend(res_2.get("items", []))

        count = 0
        for item in items:
            link = item.get("link")
            domain = link.split("//")[-1].split("/")[0].lower() # Lấy domain cơ bản
            
            # Check blacklist
            is_blocked = False
            for blocked in EXCLUDE_DOMAINS:
                if blocked in domain:
                    is_blocked = True
                    break
            
            if not is_blocked:
                results.append(link)
                count += 1
            
            if count == num_results: # Đã đủ số lượng yêu cầu
                break
                
    except Exception as e:
        print(f"Lỗi khi search từ khóa '{query}': {e}")
        
    return results

def main():
    print("--- BẮT ĐẦU JOB ---")
    
    # 1. Setup kết nối
    try:
        client = get_google_sheet_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        kw_sheet = sh.worksheet(SHEET_KW_NAME)
        pub_sheet = sh.worksheet(SHEET_PUB_NAME)
        
        api_key = os.environ.get("GOOGLE_API_KEY")
        cse_id = os.environ.get("SEARCH_ENGINE_ID")
        
        if not api_key or not cse_id:
             raise Exception("Thiếu API Key hoặc Search Engine ID")

    except Exception as e:
        print(f"Lỗi setup: {e}")
        return

    # 2. Đọc Keywords từ range B2:B
    # Lấy toàn bộ cột B, bỏ hàng đầu (Header)
    col_values = kw_sheet.col_values(2)[1:] 
    # Lọc bỏ ô trống
    keywords = [k for k in col_values if k.strip()]
    
    print(f"Tìm thấy {len(keywords)} keywords cần xử lý.")

    final_data_to_write = []
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 3. Loop và Search
    for kw in keywords:
        print(f"Đang xử lý: {kw}...")
        found_urls = google_search(kw, api_key, cse_id, num_results=10)
        
        # Chuẩn bị dữ liệu để ghi (Mỗi URL 1 dòng)
        # Format: [Thời gian, Keyword, URL, Thứ hạng]
        for idx, url in enumerate(found_urls):
            row = [current_time, kw, url, idx + 1]
            final_data_to_write.append(row)
        
        # Sleep nhẹ để tránh hit rate limit quá nhanh nếu list dài
        time.sleep(1)

    # 4. Ghi vào sheet Publisher
    if final_data_to_write:
        print(f"Đang ghi {len(final_data_to_write)} dòng vào sheet Publisher...")
        # append_rows hiệu quả hơn append_row trong vòng lặp
        pub_sheet.append_rows(final_data_to_write)
        print("Hoàn thành ghi dữ liệu.")
    else:
        print("Không tìm thấy dữ liệu nào để ghi.")

    print("--- KẾT THÚC JOB ---")

if __name__ == "__main__":
    main()
