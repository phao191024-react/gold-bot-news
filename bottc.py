import asyncio
import os
from fastapi import FastAPI
import feedparser
from google import genai
import httpx

# Khởi tạo web app (dùng cho Render và chạy cục bộ)
app = FastAPI()

# ==================== CAU HINH THONG SO (LAY TU BIEN MOI TRUONG) ====================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# Chuyển chat ID sang kiểu số nguyên (int) an toàn cho Telegram API
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

# Khởi tạo client Gemini theo chuẩn SDK mới
client = genai.Client(api_key=GEMINI_API_KEY)

# Bộ nhớ tạm để lọc tin trùng lặp (lưu tối đa 2000 tin)
seen_news_cache = set()

# Danh sách RSS chuyên sâu về Vàng (XAU/USD), Hàng hóa và Forex
GOLD_RSS_URLS = [
    "https://www.investing.com/rss/commodities.rss",
    "https://www.fxstreet.com/rss",
]


# ==================== HAM GOI GEMINI AI (CO THU LAI KHI LOI 503) ====================
async def process_news_with_gemini(raw_title: str, raw_content: str) -> str:
  prompt = f"""
    Bạn là một chuyên gia phân tích tài chính vĩ mô và hành động giá. Hãy xử lý tin tức thô sau đây bằng tiếng Việt:
    
    Tiêu đề gốc: {raw_title}
    Nội dung gốc: {raw_content}
    
    Yêu cầu đầu ra:
    1. Tiêu đề tiếng Việt ngắn gọn, thu hút.
    2. Tóm tắt cốt lõi trong 2 câu súc tích.
    3. Đánh giá mức độ tác động thị trường (Chọn 1 trong các mức: [Mạnh - High Impact], [Vừa - Medium Impact], [Nhẹ - Low Impact]) đối với Vàng (XAU/USD) hoặc Forex.
    4. Góc nhìn kỹ thuật: Nhận định định hướng phản ứng giá (khả năng quét thanh khoản, phá vỡ cấu trúc hoặc xu hướng tiếp diễn trên biểu đồ).
    
    Định dạng trả về gọn gàng, rõ ràng để đọc qua Telegram. Hạn chế dùng các ký tự đặc biệt phức tạp.
    """

  max_retries = 3
  for attempt in range(max_retries):
    try:
      response = client.models.generate_content(
          model="gemini-2.5-flash",
          contents=prompt,
      )
      return response.text
    except Exception as e:
      print(
          f"Loi khi goi Gemini API (Thu lai lan {attempt + 1}/{max_retries}):"
          f" {e}"
      )
      if attempt < max_retries - 1:
        await asyncio.sleep(6)  # Tăng thời gian chờ để nhường API
      else:
        return (
            None  # Trả về None nếu lỗi hẳn để bỏ qua, không spam tin lỗi
        )


# ==================== HAM GUI TIN QUA TELEGRAM ====================
async def send_telegram_message(text: str):
  url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
  payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}

  async with httpx.AsyncClient() as session:
    try:
      response = await session.post(url, json=payload, timeout=10.0)

      if response.status_code == 400:
        payload.pop("parse_mode", None)
        response = await session.post(url, json=payload, timeout=10.0)

      if response.status_code != 200:
        print(f"Loi gui Telegram: {response.text}")
    except Exception as e:
      print(f"Loi ket noi Telegram API: {e}")


# ==================== HAM DOC RSS FEED CHUYEN VANG ====================
async def fetch_latest_financial_news():
  news_items = []
  for url in GOLD_RSS_URLS:
    try:
      feed = await asyncio.to_thread(feedparser.parse, url)
      for entry in feed.entries:
        news_id = entry.get("id", entry.get("link"))
        title = entry.get("title", "")
        content = entry.get("summary", entry.get("description", ""))

        news_items.append({"id": news_id, "title": title, "content": content})
    except Exception as e:
      print(f"Loi doc RSS tu {url}: {e}")

  return news_items


# ==================== VONG LAP CHINH (WORKER LOOP) ====================
async def news_monitoring_worker():
  print("Bot quet tin tuc Vang/Forex dang chay lien tuc 24/7...")
  first_run = True

  while True:
    try:
      news_list = await fetch_latest_financial_news()

      # Xử lý thông minh cho lần chạy đầu tiên: Đưa hết tin hiện tại vào cache để tránh spam
      if first_run:
        for news in news_list:
          if news.get("id"):
            seen_news_cache.add(news.get("id"))
        print(
            f"-> Khởi động thành công! Đã đồng bộ {len(news_list)} tin hiện"
            " tại. Bot đang chờ tin mới thực sự..."
        )
        first_run = False
        await asyncio.sleep(60)
        continue

      # Các lần sau: Lọc ra tin thực sự mới phát sinh
      new_items_to_process = [
          news
          for news in news_list
          if news.get("id") and news.get("id") not in seen_news_cache
      ]

      if new_items_to_process:
        print(
            f"-> Phát hiện {len(new_items_to_process)} tin tức mới cần xử lý!"
        )
        for news in new_items_to_process:
          news_id = news.get("id")
          seen_news_cache.add(news_id)

          if len(seen_news_cache) > 2000:
            seen_news_cache.pop()

          print(f"Dang xu ly: {news.get('title')}")

          ai_summary = await process_news_with_gemini(
              news.get("title"), news.get("content")
          )

          # Chỉ gửi khi AI phân tích thành công (tránh gửi tin lỗi 503)
          if ai_summary:
            formatted_message = (
                f"🚨 **BREAKING GOLD & FOREX NEWS** 🚨\n\n{ai_summary}"
            )
            await send_telegram_message(formatted_message)

          # Nghỉ 5 giây giữa mỗi tin để an toàn tuyệt đối với API
          await asyncio.sleep(5)
      else:
        print("-> Không có tin mới. Đang tiếp tục quét ngầm...")
    except Exception as e:
      print(f"Lỗi vòng lặp quét tin: {e}")

    # Chờ 60 giây trước khi quét vòng tiếp theo
    await asyncio.sleep(60)


# Tự động bật worker quét tin ngầm khi khởi động ứng dụng
@app.on_event("startup")
async def startup_event():
  asyncio.create_task(news_monitoring_worker())


# Trang chủ kiểm tra trạng thái bot (Hỗ trợ đa phương thức để tránh triệt để lỗi 405 từ UptimeRobot)
@app.api_route("/", methods=["GET", "POST", "HEAD"])
def health_check():
  return {"status": "Trading News Bot is running 24/7 smoothly!"}


# Cho phép chạy thử trực tiếp trên máy cá nhân bằng lệnh `python bottc.py`
if __name__ == "__main__":
  import uvicorn

  uvicorn.run("bottc:app", host="127.0.0.1", port=8000, reload=True)
