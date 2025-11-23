#!/usr/bin/env python3
"""
Fetch transcripts for a playlist and emit static JSON files + index.json for CDN hosting (e.g., GitHub Pages).

- EN 원문 + KO 번역을 동시에 생성
- 문장 단위로 세그먼트 병합
- /transcripts/en/{videoId}.json, /transcripts/ko/{videoId}.json 구조로 저장
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Iterable, List, Optional, Dict

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

ytt_api = YouTubeTranscriptApi()


def load_env() -> dict:
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    playlist_id = os.getenv("YOUTUBE_PLAYLIST_ID")
    base_url = os.getenv("GITHUB_PAGES_BASE_URL")
    missing = [
        name
        for name, value in [
            ("YOUTUBE_API_KEY", api_key),
            ("YOUTUBE_PLAYLIST_ID", playlist_id),
            ("GITHUB_PAGES_BASE_URL", base_url),
        ]
        if not value
    ]
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


# -------- 문장 병합 로직 --------

_SENTENCE_END_RE = re.compile(r"[.!?？！。…]+$")


def merge_segments_to_sentences(
    segments: List[dict],
    max_chars: int = 120,
) -> List[dict]:
    """
    youtube-transcript-api에서 나온 세그먼트를 문장 단위로 병합.

    - 문장부호(., !, ?, …, 한중일 기호 등)를 기준으로 끊고
    - max_chars를 넘지 않도록 버퍼가 너무 길어지면 강제 플러시
    """
    merged: List[dict] = []

    buffer_texts: List[str] = []
    buffer_start: Optional[float] = None
    buffer_text_len = 0
    last_end: Optional[float] = None

    for seg in segments:
        text = (seg.get("text") or "").strip()
        if not text:
            continue

        start = float(seg.get("start", 0.0))
        duration = float(seg.get("duration", 0.0))
        end = start + duration

        if not buffer_texts:
            buffer_start = start

        buffer_texts.append(text)
        buffer_text_len += len(text) + 1
        last_end = end

        # 지금 세그먼트의 끝이 문장부호로 끝나거나, 너무 길어졌으면 한 문장으로 본다.
        is_sentence_end = bool(_SENTENCE_END_RE.search(text))
        is_too_long = buffer_text_len >= max_chars

        if is_sentence_end or is_too_long:
            merged_text = " ".join(buffer_texts).strip()
            if buffer_start is None or last_end is None:
                # 방어적 코드 (실제로는 거의 안탐)
                buffer_start = start
                last_end = end

            merged.append(
                {
                    "text": merged_text,
                    "start": buffer_start,
                    "duration": round(last_end - buffer_start, 3),
                }
            )
            # 버퍼 초기화
            buffer_texts = []
            buffer_start = None
            buffer_text_len = 0
            last_end = None

    # 마지막에 남은 버퍼 flush
    if buffer_texts and buffer_start is not None and last_end is not None:
        merged_text = " ".join(buffer_texts).strip()
        merged.append(
            {
                "text": merged_text,
                "start": buffer_start,
                "duration": round(last_end - buffer_start, 3),
            }
        )

    return merged


# -------- EN + KO 동시 생성 --------

def fetch_transcripts_en_ko(video_id: str) -> Optional[Dict[str, Optional[List[dict]]]]:
    """
    영어(en) 자막을 기준으로 가져오고, 같은 트랜스크립트를 ko로 translate 해서 둘 다 반환.

    return 형식:
    {
        "en": [ ... sentence segments ... ],
        "ko": [ ... sentence segments ... ] or None
    }
    """
    try:
        # 영어 우선으로 가져오기
        fetched_en = ytt_api.fetch(
            video_id,
            languages=["en", "en-US", "en-GB"],
        )
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable):
        print(f"[skip] transcript unavailable (no EN) for {video_id}")
        return None
    except Exception as exc:
        print(f"[error] failed to fetch EN transcript for {video_id}: {exc}")
        return None

    # EN: 문장 단위 병합
    en_raw = fetched_en.to_raw_data()
    en_sentences = merge_segments_to_sentences(en_raw)

    # KO: 번역 후 문장 단위 병합
    ko_sentences: Optional[List[dict]] = None
    try:
        fetched_ko = fetched_en.translate("ko")
        ko_raw = fetched_ko.to_raw_data()
        ko_sentences = merge_segments_to_sentences(ko_raw)
    except Exception as exc:
        # 번역이 실패해도 EN만 있어도 되므로 warning 정도만
        print(f"[warn] could not translate {video_id} → ko: {exc}")

    return {"en": en_sentences, "ko": ko_sentences}


def save_index(entries: List[dict]) -> None:
    with INDEX_PATH.open("w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
        f.write("\n")


def save_transcript(video_id: str, transcript: List[dict], lang: str) -> Path:
    """
    /transcripts/{lang}/{videoId}.json 으로 저장
    """
    lang_dir = TRANSCRIPTS_DIR / lang
    lang_dir.mkdir(parents=True, exist_ok=True)
    path = lang_dir / f"{video_id}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(transcript, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return path


def main() -> None:
    config = load_env()
    video_ids = fetch_playlist_video_ids(config["api_key"], config["playlist_id"])
    print(f"[info] playlist items: {len(video_ids)}")

    # index.json은 매번 새로 생성 (구조 바뀌었으므로)
    index_entries: List[dict] = []

    for video_id in video_ids:
        transcripts = fetch_transcripts_en_ko(video_id)
        if not transcripts:
            continue

        urls: Dict[str, str] = {}

        if transcripts.get("en"):
            save_transcript(video_id, transcripts["en"], "en")
            urls["en"] = f"{config['base_url']}/transcripts/en/{video_id}.json"

        if transcripts.get("ko"):
            save_transcript(video_id, transcripts["ko"], "ko")
            urls["ko"] = f"{config['base_url']}/transcripts/ko/{video_id}.json"

        if not urls:
            # 이론상 여기 안 오긴 함 (EN 없으면 위에서 continue 됨)
            continue

        entry = {
            "videoId": video_id,
            "transcriptUrls": urls,
        }
        index_entries.append(entry)

        print(f"[ok] saved transcript for {video_id}: {', '.join(urls.keys())}")

    save_index(index_entries)
    print("업데이트 완료")


if __name__ == "__main__":
    main()
