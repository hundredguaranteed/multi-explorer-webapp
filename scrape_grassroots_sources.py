import asyncio
import csv
import json
import os
import re
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, urljoin

from playwright.async_api import async_playwright
from playwright.async_api import TimeoutError as PlaywrightTimeoutError


ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
DATASET_URL = (
    "https://app.cerebrosports.com/portal/datasets?"
    "EventRegion=Domestic&EventLevel=PSA&EventGender=M&Sub=Youth%3EBoys%3EUSA"
)
LOGIN_URL = "https://app.cerebrosports.com/login"

EVENTS_CACHE = ROOT / "_tmp_dataset_events.json"

OUTPUT_FILES = {
    "Puma": PROJECT_ROOT / "puma_all_event_player_stats_clean.csv",
    "OTE": PROJECT_ROOT / "ote_all_event_player_stats_clean.csv",
    "Grind Session": PROJECT_ROOT / "grind_session_all_event_player_stats_clean.csv",
    "NBPA 100": PROJECT_ROOT / "nbpa_100_all_event_player_stats_clean.csv",
    "Hoophall": PROJECT_ROOT / "hoophall_all_event_player_stats_clean.csv",
    "Montverde": PROJECT_ROOT / "montverde_all_event_player_stats_clean.csv",
    "Showcases": PROJECT_ROOT / "showcases_all_event_player_stats_clean.csv",
    "EPL": PROJECT_ROOT / "epl_all_event_player_stats_clean.csv",
}

GENERAL_OUTPUT_FILE = PROJECT_ROOT / "general_hs_all_event_player_stats_clean.csv"

TABLE_HEADERS = [
    "Rank",
    "Player",
    "Team",
    "POS",
    "Class",
    "HT",
    "WT",
    "Games",
    "MIN/G",
    "RAM",
    "C-RAM",
    "USG%",
    "PSP",
    "PTS/G",
    "FG%",
    "3PE",
    "3PM/G",
    "3PT%",
    "FGS",
    "AST/G",
    "TOV",
    "ATR",
    "REB/G",
    "BLK/G",
    "DSI",
    "STL/G",
    "PF/G",
]


def normalize_name(name: str) -> str:
    return " ".join(StringPart for StringPart in name.split()).strip()


def classify_event(name: str) -> Optional[str]:
    text = name or ""
    if re.search(r"\bNike\b", text, re.I):
        if re.search(r"\bEYBL\b", text, re.I) and not re.search(r"eycl|scholastic", text, re.I):
            return "EYBL"
        return "Nike Other"
    if re.search(r"3SSB|adidas", text, re.I):
        return "3SSB"
    if re.search(r"\bUAA\b|under armour", text, re.I):
        return "UAA"
    if re.search(r"puma", text, re.I):
        return "Puma"
    if re.search(r"overtime elite|\bote\b", text, re.I):
        return "OTE"
    if re.search(r"grind session", text, re.I):
        return "Grind Session"
    if re.search(r"nbpa.*100|top 100 camp", text, re.I):
        return "NBPA 100"
    if re.search(r"hoophall|hoop hall", text, re.I):
        return "Hoophall"
    if re.search(r"montverde", text, re.I):
        return "Montverde"
    if re.search(r"elite prep league|\bepl\b", text, re.I):
        return "EPL"
    if re.search(r"showcase", text, re.I):
        return "General HS"
    return None


def parse_total_from_footer(text: str) -> Optional[int]:
    match = re.search(r"of\s+([\d,]+)", text or "")
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def csv_escape(value):
    if value is None:
        return ""
    text = str(value)
    if any(char in text for char in ['"', ",", "\n", "\r"]):
        return '"' + text.replace('"', '""') + '"'
    return text


def build_event_url(href: str) -> str:
    prefix = "/portal/events/"
    if href.startswith(prefix):
        return urljoin("https://app.cerebrosports.com", prefix + quote(href[len(prefix):], safe=""))
    return urljoin("https://app.cerebrosports.com", href)


async def login(page, email: str, password: str) -> None:
    await page.goto(LOGIN_URL, wait_until="networkidle")
    await page.wait_for_timeout(3000)
    await page.locator('input[name="email"]').fill(email)
    await page.locator('input[name="password"]').fill(password)
    await page.get_by_role("button", name="Log in").click()
    await page.wait_for_url("**/portal**", timeout=30000)
    await page.wait_for_load_state("networkidle")


async def load_events(page) -> List[Dict[str, str]]:
    if EVENTS_CACHE.exists():
        with EVENTS_CACHE.open("r", encoding="utf-8") as handle:
            cached = json.load(handle)
        if isinstance(cached, list) and cached:
            return cached

    await page.goto(DATASET_URL, wait_until="load")
    await page.wait_for_selector("a[href^='/portal/events/']")

    events: List[Dict[str, str]] = []
    while True:
        row_count = await page.locator("tr").count()
        for index in range(1, row_count):
            row = page.locator("tr").nth(index)
            cell = await row.locator("td").first.inner_text()
            name = cell.split("\n")[0].strip()
            href = await row.locator("a[href^='/portal/events/']").first.get_attribute("href")
            if not href:
                continue
            events.append({"name": name, "href": href})

        next_button = page.locator("button[aria-label='next-page']")
        if await next_button.is_disabled():
            break
        await next_button.click()
        await page.wait_for_timeout(500)
        await page.wait_for_selector("a[href^='/portal/events/']")

    with EVENTS_CACHE.open("w", encoding="utf-8") as handle:
        json.dump(events, handle, indent=2)
    return events


async def scrape_event(page, event_name: str, event_url: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    try:
        await page.goto(event_url, wait_until="load")
        await page.wait_for_load_state("domcontentloaded")
        try:
            await page.locator("tr").first.wait_for(state="attached", timeout=60000)
        except PlaywrightTimeoutError:
            await page.reload(wait_until="load")
            await page.wait_for_load_state("domcontentloaded")
            await page.locator("tr").first.wait_for(state="attached", timeout=60000)

        footer_text = ""
        footer = page.locator("text=Showing").first
        if await footer.count():
            footer_text = await footer.inner_text()
        total_players = parse_total_from_footer(footer_text)

        page_index = 1
        while True:
            header_count = await page.locator("tr").count()
            if header_count <= 1:
                break
            row_total = await page.locator("tr").count()
            for index in range(1, row_total):
                row = page.locator("tr").nth(index)
                cells = await row.locator("td").all_inner_texts()
                if not cells:
                    continue
                cells = [cell.strip() for cell in cells]
                if len(cells) < len(TABLE_HEADERS) + 1:
                    cells += [""] * (len(TABLE_HEADERS) + 1 - len(cells))
                record = {
                    "event_name": event_name,
                    "event_url": page.url,
                    "event_total_players": total_players if total_players is not None else "",
                    "page_index": page_index,
                }
                for column, value in zip(TABLE_HEADERS, cells[1 : len(TABLE_HEADERS) + 1]):
                    record[column] = value
                rows.append(record)

            next_button = page.locator("button[aria-label='next-page']")
            try:
                await next_button.wait_for(state="visible", timeout=1000)
            except PlaywrightTimeoutError:
                break
            if await next_button.is_disabled():
                break
            first_player = await page.locator("tr").nth(1).locator("td").nth(2).inner_text()
            await next_button.click()
            await page.wait_for_function(
                """before => {
                    const anchor = document.querySelector("tr:nth-child(2) td:nth-child(3) a");
                    return (anchor && anchor.textContent.trim()) !== before;
                }""",
                arg=first_player,
                timeout=10000,
            )
            page_index += 1

        if total_players is None:
            total_players = len(rows)
            for row in rows:
                row["event_total_players"] = total_players
    except PlaywrightTimeoutError as error:
        print(f"[WARN] Timeout scraping {event_name}: {error}")

    return rows


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    headers = [
        "event_name",
        "event_url",
        "event_total_players",
        "page_index",
        *TABLE_HEADERS,
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["special", "general"], default="special")
    parser.add_argument("--chunk-index", type=int, default=0)
    parser.add_argument("--chunk-count", type=int, default=1)
    parser.add_argument("--output", type=str, default="")
    args = parser.parse_args()

    email = os.environ.get("CEREBRO_EMAIL", "").strip()
    password = os.environ.get("CEREBRO_PASSWORD", "").strip()
    if not email or not password:
        raise SystemExit("Set CEREBRO_EMAIL and CEREBRO_PASSWORD before running.")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            executable_path=r"C:\Users\anu5c\AppData\Local\ms-playwright\chromium-1208\chrome-win64\chrome.exe",
        )
        page = await browser.new_page(viewport={"width": 1600, "height": 1200})
        await login(page, email, password)

        events = await load_events(page)
        if args.mode == "special":
            selected = defaultdict(list)
            for event in events:
                circuit = classify_event(event["name"])
                if circuit in OUTPUT_FILES:
                    selected[circuit].append(event)

            print("Selected event counts:")
            for circuit in OUTPUT_FILES:
                print(f"  {circuit}: {len(selected[circuit])}")

            for circuit, path in OUTPUT_FILES.items():
                rows: List[Dict[str, str]] = []
                for index, event in enumerate(selected[circuit], start=1):
                    event_url = build_event_url(event["href"])
                    print(f"[{circuit}] {index}/{len(selected[circuit])}: {event['name']}")
                    event_rows = await scrape_event(page, event["name"], event_url)
                    rows.extend(event_rows)
                write_csv(path, rows)
                print(f"Wrote {path} ({len(rows)} rows)")
        else:
            selected = [event for event in events if classify_event(event["name"]) is None]
            if not selected:
                raise SystemExit("No general events were found.")
            chunk_count = max(1, args.chunk_count)
            chunk_index = max(0, min(args.chunk_index, chunk_count - 1))
            chunk = selected[chunk_index::chunk_count]
            output_path = Path(args.output) if args.output else GENERAL_OUTPUT_FILE.with_name(
                f"{GENERAL_OUTPUT_FILE.stem}.part{chunk_index + 1}-of-{chunk_count}{GENERAL_OUTPUT_FILE.suffix}"
            )
            print(f"General events selected: {len(selected)}")
            print(f"Chunk {chunk_index + 1}/{chunk_count}: {len(chunk)} events")
            rows: List[Dict[str, str]] = []
            for index, event in enumerate(chunk, start=1):
                event_url = build_event_url(event["href"])
                print(f"[GENERAL] {index}/{len(chunk)}: {event['name']}")
                event_rows = await scrape_event(page, event["name"], event_url)
                rows.extend(event_rows)
            write_csv(output_path, rows)
            print(f"Wrote {output_path} ({len(rows)} rows)")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
