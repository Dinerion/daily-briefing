#!/usr/bin/env python3
"""
daily_briefing.py — 多语学习简报自动抓取 + 飞书推送
用法: python daily_briefing.py
环境变量:
  FEISHU_WEBHOOK_URL  飞书机器人 Webhook 地址（必填）
  FEISHU_SECRET       飞书签名校验密钥（可选）
  DRY_RUN             1 = 只抓取不推送，输出 JSON 到本地
"""

import asyncio
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from html import unescape
from textwrap import shorten
from typing import Optional

# Windows: 强制 stdout 使用 UTF-8，避免 emoji/中文乱码
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import aiohttp
import feedparser

# ============================================================
# 配置
# ============================================================
WEBHOOK_URL = os.environ.get("FEISHU_WEBHOOK_URL", "")
FEISHU_SECRET = os.environ.get("FEISHU_SECRET", "")
DRY_RUN = os.environ.get("DRY_RUN", "") == "1"

# 每个语种最终推送条数
PER_LANG = 3

# 每个 RSS 源最多抓取条数（用于排序精选）
PER_SOURCE = 6

# HTTP 请求超时与重试
REQUEST_TIMEOUT = 20
MAX_RETRIES = 2

# RSS 源定义
SOURCES: dict[str, list[dict]] = {
    "en": [
        {"name": "NPR", "url": "https://feeds.npr.org/1001/rss.xml"},
        {"name": "ABC News", "url": "https://abcnews.go.com/abcnews/topstories"},
        {"name": "CBS News", "url": "https://www.cbsnews.com/latest/rss/main"},
        {"name": "Nature", "url": "https://www.nature.com/nature.rss"},
        {"name": "BBC", "url": "https://feeds.bbci.co.uk/news/rss.xml"},
        {"name": "Reuters", "url": "https://feeds.reuters.com/reuters/topNews"},
        {"name": "The Guardian", "url": "https://www.theguardian.com/world/rss"},
    ],
    "fr": [
        {"name": "France Info", "url": "https://www.francetvinfo.fr/titres.rss"},
        {"name": "France 24", "url": "https://www.france24.com/fr/rss"},
        {"name": "Le Figaro", "url": "https://www.lefigaro.fr/rss/figaro_actualites.xml"},
        {"name": "20 Minutes", "url": "https://www.20minutes.fr/feeds/rss-une.xml"},
        {"name": "TV5Monde", "url": "https://information.tv5monde.com/rss"},
    ],
    "ja": [
        {"name": "NHK", "url": "https://www.nhk.or.jp/rss/news/cat0.xml"},
        {"name": "朝日新聞", "url": "http://www.asahi.com/rss/asahi/newsheadlines.rdf"},
        {"name": "毎日新聞", "url": "https://mainichi.jp/rss/etc/mainichi-flash.rss"},
        {"name": "読売新聞", "url": "https://www.yomiuri.co.jp/rss/"},
    ],
    "tech": [
        {"name": "Hacker News", "url": "https://hnrss.org/frontpage?count=10"},
        {"name": "MIT Tech Review", "url": "https://www.technologyreview.com/feed/"},
        {"name": "Ars Technica", "url": "https://feeds.arstechnica.com/arstechnica/index"},
        {"name": "TechCrunch", "url": "https://techcrunch.com/feed/"},
    ],
}

# 语言标签
LANG_LABELS = {
    "en": "🇬🇧 英语新闻",
    "fr": "🇫🇷 Actualités Françaises",
    "ja": "🇯🇵 日本語ニュース",
    "tech": "🤖 AI / 科技科普",
}


# ============================================================
# 工具函数
# ============================================================
def clean_html(raw: str) -> str:
    """去掉 HTML 标签，解码实体，压缩空白"""
    text = re.sub(r"<[^>]+>", "", raw)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_image(entry) -> Optional[str]:
    """从 RSS entry 中提取图片链接"""
    if hasattr(entry, "media_content") and entry.media_content:
        for m in entry.media_content:
            if m.get("url"):
                return m["url"]
    if hasattr(entry, "links"):
        for link in entry.links:
            if link.get("type", "").startswith("image"):
                return link.get("href", "")
    img_match = re.search(
        r'<img[^>]+src=["\']([^"\']+)["\']',
        entry.get("summary", entry.get("description", ""))
    )
    if img_match:
        return img_match.group(1)
    return None


def parse_date(entry) -> Optional[datetime]:
    """尝试从 entry 中提取标准化 UTC 时间"""
    for attr in ("published_parsed", "updated_parsed"):
        tp = getattr(entry, attr, None)
        if tp:
            try:
                return datetime(*tp[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                pass
    return None


# ============================================================
# RSS 抓取
# ============================================================
async def fetch_feed(
    session: aiohttp.ClientSession,
    source: dict,
    sem: asyncio.Semaphore,
) -> list[dict]:
    """抓取单个 RSS 源，带重试与限流"""
    name, url = source["name"], source["url"]
    async with sem:
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    if resp.status != 200:
                        print(f"  ⚠ {name}: HTTP {resp.status}")
                        return []
                    xml = await resp.text()
                break  # 成功则跳出重试循环
            except Exception as e:
                if attempt < MAX_RETRIES:
                    wait = 2 ** attempt
                    print(f"  ↻ {name}: 重试 {attempt + 1}/{MAX_RETRIES}，等待 {wait}s …")
                    await asyncio.sleep(wait)
                else:
                    print(f"  ✗ {name}: {e}")
                    return []

    feed = feedparser.parse(xml)
    if feed.bozo and feed.bozo_exception:
        err_msg = str(feed.bozo_exception)[:80]
        print(f"  ⚠ {name}: parse warning — {err_msg}")

    items = []
    for entry in feed.entries[:PER_SOURCE]:
        title = clean_html(entry.get("title", ""))
        summary = clean_html(entry.get("summary", entry.get("description", "")))
        link = entry.get("link", "")

        if not title or len(title) < 6:
            continue

        pub_date = parse_date(entry)

        items.append({
            "title": title.strip(),
            "summary": shorten(summary, width=260, placeholder="…") if summary else "",
            "link": link,
            "source": name,
            "image": extract_image(entry),
            "published": pub_date.isoformat() if pub_date else None,
            "published_ts": pub_date.timestamp() if pub_date else 0,
        })

    print(f"  ✓ {name}: {len(items)} 条")
    return items


async def fetch_all() -> dict[str, list[dict]]:
    """并行抓取所有源，合并去重排序"""
    results: dict[str, list[dict]] = {}
    sem = asyncio.Semaphore(6)  # 最多 6 个并发请求
    async with aiohttp.ClientSession(
        headers={"User-Agent": "DailyBriefing/1.0 (language-learning bot)"}
    ) as session:
        for lang, src_list in SOURCES.items():
            print(f"\n📡 抓取 {LANG_LABELS[lang]} …")
            tasks = [fetch_feed(session, s, sem) for s in src_list]
            all_items = await asyncio.gather(*tasks)

            # ---- 合并 + 去重 + 排序 ----
            merged: list[dict] = []
            seen_links: set[str] = set()
            seen_titles: set[str] = set()

            for item_list in all_items:
                for item in item_list:
                    link = item["link"]
                    title_key = item["title"][:40]

                    # 主键：link 去重
                    if link and link in seen_links:
                        continue
                    # 次键：title 相似度去重
                    if title_key in seen_titles:
                        continue

                    if link:
                        seen_links.add(link)
                    seen_titles.add(title_key)
                    merged.append(item)

            # 按发布时间倒序，无时间的排最后
            merged.sort(key=lambda x: x.get("published_ts", 0), reverse=True)

            results[lang] = merged[:PER_LANG]

    return results


# ============================================================
# 飞书卡片构建
# ============================================================
def build_card(results: dict[str, list[dict]]) -> dict:
    """构建折叠式飞书卡片 JSON（v2 结构，兼容新版飞书）"""
    from datetime import timedelta
    beijing_tz = timezone(timedelta(hours=8))
    beijing_time = datetime.now(timezone.utc).astimezone(beijing_tz)
    date_str = beijing_time.strftime("%Y年%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][beijing_time.weekday()]
    day_label = f"{date_str} {weekday}"

    elements = []
    item_idx = 0

    for lang in ["en", "fr", "ja", "tech"]:
        items = results.get(lang, [])
        if not items:
            continue

        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**{LANG_LABELS[lang]}** （{len(items)}条）"
            }
        })
        elements.append({"tag": "hr"})

        for item in items:
            item_idx += 1
            title = item["title"]
            summary = item["summary"]
            source = item["source"]
            link = item["link"]

            # 标题行：序号 + 标题 + 链接
            title_md = f"**{item_idx}.** [{title}]({link})"
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": title_md
                }
            })

            # 摘要 + 来源作为 note
            note_lines = []
            if summary:
                note_lines.append(summary)
            if item.get("published"):
                pub_short = item["published"][:16].replace("T", " ")
                note_lines.append(f"🕐 {pub_short} UTC")
            note_lines.append(f"📍 来源：{source}")
            note_text = "  ·  ".join(note_lines)

            elements.append({
                "tag": "note",
                "elements": [{
                    "tag": "plain_text",
                    "content": note_text
                }]
            })

        elements.append({"tag": "hr"})

    # 去掉末尾重复的 hr（最后一种语言结束时产生一对 hr）
    while elements and elements[-1].get("tag") == "hr":
        elements.pop()

    total = sum(len(v) for v in results.values())
    if total == 0:
        elements = [{
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "⚠️ 今天未抓取到内容，请检查 RSS 源是否可访问。"
            }
        }]

    elements.append({"tag": "hr"})
    elements.append({
        "tag": "note",
        "elements": [{
            "tag": "plain_text",
            "content": (
                f"📊 共 {total} 条 · 自动抓取于 {day_label} 北京时间 · "
                "来源：NPR/BBC/Reuters/Guardian · France Info/TV5M/Le Monde/RFI · "
                "NHK/毎日/朝日/読売 · HN/MIT TR/Ars/TC"
            )
        }]
    })

    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {
                "title": {"tag": "plain_text", "content": f"📰 多语学习简报 | {day_label}"},
                "template": "indigo",
            },
            "elements": elements,
        },
    }


# ============================================================
# 签名（飞书安全设置，可选）
# ============================================================
def generate_sign(timestamp: str) -> str:
    """飞书签名校验: base64(hmac-sha256(timestamp + '\n' + secret))"""
    import base64
    import hashlib
    import hmac

    h = hmac.new(
        FEISHU_SECRET.encode("utf-8"),
        f"{timestamp}\n{FEISHU_SECRET}".encode("utf-8"),
        hashlib.sha256,
    )
    return base64.b64encode(h.digest()).decode("utf-8")


# ============================================================
# 发送
# ============================================================
async def send_card(card: dict) -> bool:
    """POST 到飞书 Webhook，若设置了 secret 则加签名"""
    if not WEBHOOK_URL:
        print("❌ 未设置 FEISHU_WEBHOOK_URL 环境变量")
        return False

    if DRY_RUN:
        out_path = os.path.join(tempfile.gettempdir(), "daily_briefing_card.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(card, f, ensure_ascii=False, indent=2)
        print(f"\n📝 DRY_RUN 模式 — 卡片已保存到 {out_path}")
        return True

    payload = json.dumps(card, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json; charset=utf-8"}

    # 签名
    if FEISHU_SECRET:
        ts = str(int(datetime.now(timezone.utc).timestamp()))
        headers["X-Lark-Signature"] = generate_sign(ts)
        headers["X-Lark-Request-Timestamp"] = ts

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                WEBHOOK_URL,
                data=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                result = await resp.json()
                code = result.get("code", -1)
                msg = result.get("msg", "")
                print(f"\n📨 推送结果: code={code} — {msg}")
                return code == 0
        except Exception as e:
            print(f"\n❌ 推送失败: {e}")
            return False


# ============================================================
# 主流程
# ============================================================
async def main():
    print("=" * 56)
    print("📰 多语学习简报 — Daily Briefing")
    print(f"   语种: {', '.join(LANG_LABELS.values())}")
    print(f"   每语种上限: {PER_LANG} 条 | 每源上限: {PER_SOURCE} 条")
    if DRY_RUN:
        print("   🔍 DRY_RUN 模式（不推送）")
    print("=" * 56)

    results = await fetch_all()

    print("\n" + "=" * 56)
    print("📊 抓取结果：")
    total = 0
    for lang, items in results.items():
        print(f"  {LANG_LABELS[lang]}: {len(items)} 条")
        total += len(items)
    print(f"  合计: {total} 条")

    if total == 0:
        print("❌ 无内容可推送，退出。")
        return

    card = build_card(results)
    success = await send_card(card)
    if success:
        print("✅ 简报推送成功！")
    else:
        print("❌ 推送失败。")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
