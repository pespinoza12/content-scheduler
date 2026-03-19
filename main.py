"""
AstroBot Content Scheduler
Publishes pre-produced content to Instagram + Facebook on schedule.

- Checks every hour if there's content to publish for today
- Reads schedule from /data/schedule.json
- Publishes photos, carousels, and reels
- Marks items as published after success
- Small FastAPI for health check + schedule management
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager

import base64
import httpx
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

# Config
GRAPH_API = "https://graph.facebook.com/v21.0"
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "")
IG_USER_ID = os.getenv("INSTAGRAM_USER_ID", "17841438980231709")
FB_PAGE_ID = os.getenv("FACEBOOK_PAGE_ID", "993351410533522")
CHECK_HOUR = int(os.getenv("PUBLISH_HOUR", "9"))  # Hour in UTC to publish (9 = 6AM BRT)
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
SCHEDULE_FILE = DATA_DIR / "schedule.json"
GEMINI_HUB_URL = os.getenv("GEMINI_HUB_URL", "https://gemini-rag-api.pespinoza.online")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scheduler")


# ─── Schedule Data ───────────────────────────────────────────────

async def upload_to_drive(file_bytes: bytes, filename: str, mime_type: str = "image/png") -> str:
    """Upload file to Google Drive via gemini-hub and return permanent public URL."""
    b64 = base64.standard_b64encode(file_bytes).decode("ascii")
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Use MCP server's drive_upload via SSE/HTTP
        r = await client.post(
            f"{GEMINI_HUB_URL}/call/drive_upload",
            json={
                "filename": filename,
                "content_base64": b64,
                "mime_type": mime_type,
                "make_public": True,
            },
        )
        data = r.json()
        file_id = data.get("id", "")
        if file_id:
            return f"https://drive.google.com/uc?export=download&id={file_id}"

        # Fallback: try the upload-image endpoint (temporary but works)
        logger.warning(f"Drive upload failed: {data}. Using temporary server URL.")
        return ""


def load_schedule() -> list[dict]:
    if SCHEDULE_FILE.exists():
        return json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))
    return []


def save_schedule(items: list[dict]):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SCHEDULE_FILE.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")


# ─── Publishing Functions ────────────────────────────────────────

async def publish_ig_photo(client: httpx.AsyncClient, image_url: str, caption: str) -> dict:
    """Publish photo to Instagram via 2-phase."""
    # Create container
    r = await client.post(f"{GRAPH_API}/{IG_USER_ID}/media", data={
        "image_url": image_url, "caption": caption, "access_token": ACCESS_TOKEN,
    })
    data = r.json()
    if "id" not in data:
        return {"success": False, "error": data.get("error", {}).get("message", str(data)), "platform": "instagram"}
    container_id = data["id"]
    await asyncio.sleep(3)
    # Publish
    r2 = await client.post(f"{GRAPH_API}/{IG_USER_ID}/media_publish", data={
        "creation_id": container_id, "access_token": ACCESS_TOKEN,
    })
    data2 = r2.json()
    if "id" in data2:
        return {"success": True, "media_id": data2["id"], "platform": "instagram"}
    return {"success": False, "error": data2.get("error", {}).get("message", str(data2)), "platform": "instagram"}


async def publish_ig_carousel(client: httpx.AsyncClient, image_urls: list[str], caption: str) -> dict:
    """Publish carousel to Instagram."""
    children = []
    for url in image_urls:
        r = await client.post(f"{GRAPH_API}/{IG_USER_ID}/media", data={
            "image_url": url, "is_carousel_item": "true", "access_token": ACCESS_TOKEN,
        })
        if "id" in r.json():
            children.append(r.json()["id"])
        await asyncio.sleep(2)

    r = await client.post(f"{GRAPH_API}/{IG_USER_ID}/media", data={
        "media_type": "CAROUSEL", "children": ",".join(children),
        "caption": caption, "access_token": ACCESS_TOKEN,
    })
    if "id" not in r.json():
        return {"success": False, "error": str(r.json()), "platform": "instagram"}
    container_id = r.json()["id"]
    await asyncio.sleep(3)
    r2 = await client.post(f"{GRAPH_API}/{IG_USER_ID}/media_publish", data={
        "creation_id": container_id, "access_token": ACCESS_TOKEN,
    })
    if "id" in r2.json():
        return {"success": True, "media_id": r2.json()["id"], "platform": "instagram"}
    return {"success": False, "error": str(r2.json()), "platform": "instagram"}


async def publish_ig_reel(client: httpx.AsyncClient, video_url: str, caption: str) -> dict:
    """Publish reel to Instagram with polling."""
    r = await client.post(f"{GRAPH_API}/{IG_USER_ID}/media", data={
        "media_type": "REELS", "video_url": video_url,
        "caption": caption, "share_to_feed": "true", "access_token": ACCESS_TOKEN,
    })
    data = r.json()
    if "id" not in data:
        return {"success": False, "error": str(data), "platform": "instagram"}
    container_id = data["id"]

    # Poll until ready
    for _ in range(30):
        await asyncio.sleep(10)
        check = await client.get(f"{GRAPH_API}/{container_id}", params={
            "fields": "status_code", "access_token": ACCESS_TOKEN,
        })
        status = check.json().get("status_code", "IN_PROGRESS")
        if status == "FINISHED":
            break
        if status == "ERROR":
            return {"success": False, "error": "Video processing failed", "platform": "instagram"}

    r2 = await client.post(f"{GRAPH_API}/{IG_USER_ID}/media_publish", data={
        "creation_id": container_id, "access_token": ACCESS_TOKEN,
    })
    if "id" in r2.json():
        return {"success": True, "media_id": r2.json()["id"], "platform": "instagram"}
    return {"success": False, "error": str(r2.json()), "platform": "instagram"}


async def publish_fb_photo(client: httpx.AsyncClient, image_url: str, caption: str) -> dict:
    """Publish photo to Facebook Page."""
    r = await client.post(f"{GRAPH_API}/{FB_PAGE_ID}/photos", data={
        "url": image_url, "message": caption, "access_token": ACCESS_TOKEN,
    })
    data = r.json()
    if "id" in data:
        return {"success": True, "post_id": data["id"], "platform": "facebook"}
    return {"success": False, "error": data.get("error", {}).get("message", str(data)), "platform": "facebook"}


async def publish_fb_video(client: httpx.AsyncClient, video_url: str, caption: str) -> dict:
    """Publish video to Facebook Page."""
    r = await client.post(f"{GRAPH_API}/{FB_PAGE_ID}/videos", data={
        "file_url": video_url, "description": caption, "access_token": ACCESS_TOKEN,
    }, timeout=300.0)
    data = r.json()
    if "id" in data:
        return {"success": True, "video_id": data["id"], "platform": "facebook"}
    return {"success": False, "error": data.get("error", {}).get("message", str(data)), "platform": "facebook"}


# ─── Scheduler Logic ─────────────────────────────────────────────

async def process_scheduled_item(client: httpx.AsyncClient, item: dict) -> dict:
    """Process a single scheduled item."""
    content_type = item.get("type", "photo")  # photo, carousel, reel
    caption = item.get("caption", "")
    platforms = item.get("platforms", ["instagram", "facebook"])
    results = {}

    if content_type == "photo":
        image_url = item.get("image_url", "")
        if "instagram" in platforms:
            results["instagram"] = await publish_ig_photo(client, image_url, caption)
            await asyncio.sleep(3)
        if "facebook" in platforms:
            results["facebook"] = await publish_fb_photo(client, image_url, caption)

    elif content_type == "carousel":
        image_urls = item.get("image_urls", [])
        if "instagram" in platforms:
            results["instagram"] = await publish_ig_carousel(client, image_urls, caption)
            await asyncio.sleep(3)
        # Facebook: publish first image as photo with caption
        if "facebook" in platforms and image_urls:
            results["facebook"] = await publish_fb_photo(client, image_urls[0], caption)

    elif content_type == "reel":
        video_url = item.get("video_url", "")
        if "instagram" in platforms:
            results["instagram"] = await publish_ig_reel(client, video_url, caption)
            await asyncio.sleep(3)
        if "facebook" in platforms:
            results["facebook"] = await publish_fb_video(client, video_url, caption)

    return results


async def cleanup_image(client: httpx.AsyncClient, url: str):
    """Delete image from gemini-hub server after successful publish."""
    if not url or "gemini-rag-api" not in url:
        return  # Only clean up images hosted on our server
    filename = url.split("/")[-1]
    try:
        r = await client.delete(f"{GEMINI_HUB_URL}/images/{filename}", timeout=10.0)
        data = r.json()
        if data.get("status") == "deleted":
            logger.info(f"  Cleanup: {filename} deleted from server")
        else:
            logger.warning(f"  Cleanup: {filename} — {data}")
    except Exception as e:
        logger.warning(f"  Cleanup failed for {filename}: {e}")


async def run_scheduler():
    """Check schedule and publish content for today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    schedule = load_schedule()
    pending = [item for item in schedule if item.get("date") == today and not item.get("published")]

    if not pending:
        logger.info(f"No content scheduled for {today}")
        return

    logger.info(f"Found {len(pending)} items to publish for {today}")

    async with httpx.AsyncClient(timeout=120.0) as client:
        for item in pending:
            title = item.get("title", item.get("type", "unknown"))
            logger.info(f"Publishing: {title}")
            try:
                results = await process_scheduled_item(client, item)
                item["published"] = True
                item["published_at"] = datetime.now(timezone.utc).isoformat()
                item["results"] = results
                logger.info(f"  Results: {json.dumps(results, default=str)[:200]}")

                # Cleanup: delete images from server after successful publish
                all_success = all(
                    r.get("success", False) for r in results.values() if isinstance(r, dict)
                )
                if all_success:
                    # Clean up all URLs used
                    for url in [item.get("image_url", "")] + (item.get("image_urls") or []) + [item.get("video_url", "")]:
                        if url:
                            await cleanup_image(client, url)

            except Exception as e:
                logger.error(f"  Error: {e}")
                item["error"] = str(e)
            await asyncio.sleep(5)

    save_schedule(schedule)
    logger.info("Schedule updated")


async def scheduler_loop():
    """Background loop that checks every hour."""
    logger.info(f"Scheduler started. Will publish at {CHECK_HOUR}:00 UTC daily.")
    last_run_date = None

    while True:
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")

        # Run once per day at the configured hour
        if now.hour >= CHECK_HOUR and last_run_date != today:
            logger.info(f"Running scheduled publish for {today}...")
            try:
                await run_scheduler()
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            last_run_date = today

        await asyncio.sleep(300)  # Check every 5 minutes


# ─── FastAPI App ─────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start scheduler in background
    task = asyncio.create_task(scheduler_loop())
    yield
    task.cancel()

app = FastAPI(title="AstroBot Content Scheduler", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class ScheduleItem(BaseModel):
    date: str  # YYYY-MM-DD
    title: str
    type: str = "photo"  # photo, carousel, reel
    caption: str = ""
    image_url: Optional[str] = None
    image_urls: Optional[list[str]] = None
    video_url: Optional[str] = None
    platforms: list[str] = ["instagram", "facebook"]


@app.get("/")
async def root():
    schedule = load_schedule()
    pending = [i for i in schedule if not i.get("published")]
    published = [i for i in schedule if i.get("published")]
    return {
        "service": "astrobot-content-scheduler",
        "status": "running",
        "publish_hour_utc": CHECK_HOUR,
        "total_scheduled": len(schedule),
        "pending": len(pending),
        "published": len(published),
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/schedule")
async def get_schedule():
    return load_schedule()


@app.get("/schedule/pending")
async def get_pending():
    schedule = load_schedule()
    return [i for i in schedule if not i.get("published")]


@app.post("/schedule")
async def add_to_schedule(item: ScheduleItem):
    schedule = load_schedule()
    entry = item.model_dump()
    entry["published"] = False
    entry["created_at"] = datetime.now(timezone.utc).isoformat()
    schedule.append(entry)
    save_schedule(schedule)
    return {"status": "added", "total_pending": sum(1 for i in schedule if not i.get("published"))}


@app.post("/schedule/bulk")
async def add_bulk(items: list[ScheduleItem]):
    schedule = load_schedule()
    for item in items:
        entry = item.model_dump()
        entry["published"] = False
        entry["created_at"] = datetime.now(timezone.utc).isoformat()
        schedule.append(entry)
    save_schedule(schedule)
    return {"status": "added", "count": len(items), "total_pending": sum(1 for i in schedule if not i.get("published"))}


@app.delete("/schedule/{index}")
async def remove_from_schedule(index: int):
    schedule = load_schedule()
    if index < 0 or index >= len(schedule):
        raise HTTPException(404, "Index out of range")
    removed = schedule.pop(index)
    save_schedule(schedule)
    return {"status": "removed", "item": removed.get("title", "")}


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload image to Google Drive and return permanent URL."""
    content = await file.read()
    filename = file.filename or "image.png"
    ext = Path(filename).suffix.lower()
    mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "mp4": "video/mp4"}.get(ext.lstrip("."), "image/png")

    url = await upload_to_drive(content, f"astrobot-{filename}", mime)
    if not url:
        raise HTTPException(500, "Failed to upload to Drive")
    return {"url": url, "filename": filename, "size_bytes": len(content)}


@app.post("/schedule-with-upload")
async def schedule_with_upload(
    file: UploadFile = File(...),
    date: str = Form(...),
    title: str = Form(...),
    caption: str = Form(""),
    type: str = Form("photo"),
    platforms: str = Form("instagram,facebook"),
):
    """Upload image to Drive and schedule it in one step."""
    content = await file.read()
    filename = file.filename or "image.png"
    ext = Path(filename).suffix.lower()
    mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg"}.get(ext.lstrip("."), "image/png")

    url = await upload_to_drive(content, f"astrobot-{date}-{title.replace(' ', '_')[:30]}{ext}", mime)
    if not url:
        raise HTTPException(500, "Failed to upload to Drive")

    schedule = load_schedule()
    entry = {
        "date": date,
        "title": title,
        "type": type,
        "caption": caption,
        "image_url": url,
        "image_urls": None,
        "video_url": None,
        "platforms": [p.strip() for p in platforms.split(",")],
        "published": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    schedule.append(entry)
    save_schedule(schedule)
    return {"status": "uploaded_and_scheduled", "url": url, "date": date, "title": title}


@app.post("/publish-now")
async def publish_now():
    """Force publish all pending items for today."""
    await run_scheduler()
    return {"status": "done"}


@app.post("/publish-now/{index}")
async def publish_single(index: int):
    """Force publish a specific item by index."""
    schedule = load_schedule()
    if index < 0 or index >= len(schedule):
        raise HTTPException(404, "Index out of range")
    item = schedule[index]
    if item.get("published"):
        return {"status": "already_published"}

    async with httpx.AsyncClient(timeout=120.0) as client:
        results = await process_scheduled_item(client, item)
        item["published"] = True
        item["published_at"] = datetime.now(timezone.utc).isoformat()
        item["results"] = results

    save_schedule(schedule)
    return {"status": "published", "results": results}
