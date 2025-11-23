#!/usr/bin/env python3
"""
Fetch transcripts for a playlist and emit static JSON files + index.json for CDN hosting (e.g., GitHub Pages).

- Generates EN original + KO translated in one pass
- Splits into sentence-level segments (handles multiple sentences in one subtitle line)
- Saves to /transcripts/en/{videoId}.json and /transcripts/ko/{videoId}.json
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv
from youtube_transcript_api import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
    YouTubeTranscriptApi,
)

BASE_DIR = Path(__file__).resolve().parent
TRANSCRIPTS_DIR = BASE_DIR / "transcripts"
INDEX_PATH = BASE_DIR / "index.json"
PLAYLIST_ITEMS_URL = "https://www.googleapis.com/youtube/v3/playlistItems"

ytt_api = YouTubeTranscriptApi()

# Sentence helpers
_SENTENCE_END_RE = re.compile(r"[.!?？！。…]+$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?？！。…])\s+")


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


def merge_segments_to_sentences(segments: List[dict], max_chars: int = 120) -> List[dict]:
    """
    Convert raw transcript segments into sentence-level segments.

    - Splits a single subtitle line into sentences when multiple exist.
    - Distributes duration proportionally by sentence length.
    - Merges buffered text until sentence-ending punctuation or max_chars threshold.
    """
    merged: List[dict] = []

    buffer_texts: List[str] = []
    buffer_start: Optional[float] = None
    buffer_text_len = 0
    last_end: Optional[float] = None

    for seg in segments:
        raw_text = (seg.get("text") or "").strip()
        if not raw_text:
            continue

        start = float(seg.get("start", 0.0))
        duration = float(seg.get("duration", 0.0))
        end = start + duration

        sentences = [s.strip() for s in _SENTENCE_SPLIT_RE.split(raw_text) if s.strip()]
        if not sentences:
            continue

        total_len = sum(len(s) for s in sentences) or len(sentences)
        sentence_starts: List[float] = []
        sentence_durations: List[float] = []
        cursor = start
        for idx, s in enumerate(sentences):
            if idx == len(sentences) - 1:
                seg_duration = end - cursor
            else:
                ratio = len(s) / total_len
                seg_duration = duration * ratio
            sentence_starts.append(cursor)
            sentence_durations.append(seg_duration)
            cursor += seg_duration

        for s_text, s_start, s_dur in zip(sentences, sentence_starts, sentence_durations):
            s_end = s_start + s_dur
            if not buffer_texts:
                buffer_start = s_start

            buffer_texts.append(s_text)
            buffer_text_len += len(s_text) + 1
            last_end = s_end

            is_sentence_end = bool(_SENTENCE_END_RE.search(s_text))
            is_too_long = buffer_text_len >= max_chars

            if is_sentence_end or is_too_long:
                merged_text = " ".join(buffer_texts).strip()
                if buffer_start is None or last_end is None:
                    buffer_start = s_start
                    last_end = s_end

                merged.append(
                    {
                        "text": merged_text,
                        "start": buffer_start,
                        "duration": round(last_end - buffer_start, 3),
                    }
                )
                buffer_texts = []
                buffer_start = None
                buffer_text_len = 0
                last_end = None

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


def fetch_transcripts_en_ko(video_id: str) -> Optional[Dict[str, Optional[List[dict]]]]:
    """
    영어(en) 기준으로 가져오고 ko 번역도 생성.
    """
    try:
        fetched_en = ytt_api.fetch(video_id, languages=["en", "en-US", "en-GB"])
    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable):
        print(f"[skip] transcript unavailable (no EN) for {video_id}")
        return None
    except Exception as exc:
        print(f"[error] failed to fetch EN transcript for {video_id}: {exc}")
        return None

    en_sentences = merge_segments_to_sentences(fetched_en.to_raw_data())

    ko_sentences: Optional[List[dict]] = None
    try:
        fetched_ko = fetched_en.translate("ko")
        ko_sentences = merge_segments_to_sentences(fetched_ko.to_raw_data())
    except Exception as exc:
        print(f"[warn] could not translate {video_id} → ko: {exc}")

    return {"en": en_sentences, "ko": ko_sentences}


def save_index(entries: List[dict]) -> None:
    with INDEX_PATH.open("w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
        f.write("\n")


def save_transcript(video_id: str, transcript: List[dict], lang: str) -> Path:
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
