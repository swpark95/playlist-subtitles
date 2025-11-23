#!/usr/bin/env python3
"""
Fetch transcripts for a playlist and emit static JSON files + index.json for CDN hosting (e.g., GitHub Pages).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable, List, Optional

import requests
from dotenv import load_dotenv
from youtube_transcript_api import (
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
    YouTubeTranscriptApi,
)


BASE_DIR = Path(__file__).resolve().parent
TRANSCRIPTS_DIR = BASE_DIR / "transcripts"
INDEX_PATH = BASE_DIR / "index.json"
PLAYLIST_ITEMS_URL = "https://www.googleapis.com/youtube/v3/playlistItems"


def load_env() -> dict:
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    playlist_id = os.getenv("YOUTUBE_PLAYLIST_ID")
    base_url = os.getenv("GITHUB_PAGES_BASE_URL")
    missing = [name for name, value in [
        ("YOUTUBE_API_KEY", api_key),
        ("YOUTUBE_PLAYLIST_ID", playlist_id),
        ("GITHUB_PAGES_BASE_URL", base_url),
    ] if not value]
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")
    return {
        "api_key": api_key,
        "playlist_id": playlist_id,
        "base_url": base_url.rstrip("/"),
    }


def fetch_playlist_video_ids(api_key: str, playlist_id: str) -> List[str]:
    ids: List[str] = []
    page_token: Optional[str] = None
    while True:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token
        resp = requests.get(PLAYLIST_ITEMS_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("items", []):
            video_id = item.get("contentDetails", {}).get("videoId")
            if video_id:
                ids.append(video_id)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return ids


# ---------------------- 수정된 부분 시작 ----------------------
ytt_api = YouTubeTranscriptApi()

def fetch_transcript(video_id: str, languages: Iterable[str]) -> Optional[List[dict]]:
    try:
        fetched = ytt_api.fetch(video_id, languages=list(languages))  # ← get_transcript 제거
        return fetched.to_raw_data()  # ← FetchedTranscript → list[dict]
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable):
        print(f"[skip] transcript unavailable for {video_id}")
        return None
    except Exception as exc:
        print(f"[error] failed to fetch transcript for {video_id}: {exc}")
        return None
# ---------------------- 수정된 부분 끝 ----------------------


def load_index() -> List[dict]:
    if INDEX_PATH.exists():
        try:
            with INDEX_PATH.open("r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("[warn] index.json is invalid JSON; starting fresh")
    return []


def save_index(entries: List[dict]) -> None:
    with INDEX_PATH.open("w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
        f.write("\n")


def save_transcript(video_id: str, transcript: List[dict]) -> Path:
    TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
    path = TRANSCRIPTS_DIR / f"{video_id}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(transcript, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return path


def main() -> None:
    config = load_env()
    existing_index = load_index()
    processed_ids = {entry.get("videoId") for entry in existing_index if "videoId" in entry}

    video_ids = fetch_playlist_video_ids(config["api_key"], config["playlist_id"])
    print(f"[info] playlist items: {len(video_ids)}")

    languages = ["en", "en-US", "en-GB", "ko", "ko-KR"]
    new_entries: List[dict] = []

    for video_id in video_ids:
        if video_id in processed_ids:
            print(f"[skip] already processed {video_id}")
            continue

        transcript = fetch_transcript(video_id, languages)
        if not transcript:
            continue

        save_transcript(video_id, transcript)
        entry = {
            "videoId": video_id,
            "transcriptUrl": f"{config['base_url']}/transcripts/{video_id}.json",
        }
        new_entries.append(entry)
        existing_index.append(entry)
        processed_ids.add(video_id)
        print(f"[ok] saved transcript for {video_id}")

    save_index(existing_index)
    print("업데이트 완료")


if __name__ == "__main__":
    main()
