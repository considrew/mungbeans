"""
Patent Data Downloader & Processor
====================================
Downloads PatentsView bulk data from the USPTO Open Data Portal,
extracts the two tables we need (patents + assignees), and builds
a lightweight JSON lookup file for the micro cap screener.

WHAT IT DOES:
  1. Hits the ODP API to get download URLs for the bulk data files
  2. Downloads only g_patent.tsv and g_assignee_disambiguated.tsv
  3. Filters to patents from the last 3 years
  4. Joins patent -> assignee
  5. Outputs patent_lookup.json (company name -> patent count + titles)

RUN THIS QUARTERLY when PatentsView updates their data.

SETUP:
  export USPTO_API_KEY="your_key_here"
  pip install requests
  python scripts/microcap/patent_data_downloader.py

OUTPUT:
  data/microcap_innovation/patent_lookup.json
"""

import requests
import json
import csv
import os
import zipfile
import io
from datetime import datetime, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
USPTO_API_KEY = os.environ.get("USPTO_API_KEY", "")
OUTPUT_DIR = "data/microcap_innovation"
DOWNLOAD_DIR = "data/patent_bulk"
PATENT_LOOKBACK_YEARS = 3

# ODP API endpoint for PatentsView Granted Patent Disambiguated Data
ODP_API_BASE = "https://api.uspto.gov/api/v1/datasets/products/PVGPATDIS"

# The two files we need from the bulk dataset
NEEDED_FILES = [
    "g_patent",              # patent_id, patent_date, patent_title, patent_type
    "g_assignee_disambiguated"  # patent_id, assignee_id, disambig_assignee_organization
]


# ---------------------------------------------------------------------------
# STEP 1: Get file download URLs from ODP API
# ---------------------------------------------------------------------------
def get_file_urls(api_key):
    """
    Query the ODP API to get download URLs for the bulk data files.
    Returns a dict of filename -> download_url.
    """
    print("Querying ODP API for file listing...")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": api_key
    }

    params = {
        "fileDataFromDate": "1976-01-01",
        "fileDataToDate": datetime.now().strftime("%Y-%m-%d"),
        "includeFiles": "true"
    }

    resp = requests.get(ODP_API_BASE, headers=headers, params=params, timeout=30)

    if resp.status_code != 200:
        print(f"  API returned {resp.status_code}: {resp.text}")
        print("\n  If you're getting 'Forbidden', try running this on your")
        print("  local machine — the API may have IP restrictions.")
        return None

    data = resp.json()

    # Parse the response to find download URLs
    file_urls = {}

    # The ODP API nests files under bulkDataProductBag -> productFileBag -> fileDataBag
    files = data.get("files", data.get("productFiles", []))

    # Handle the nested bulkDataProductBag structure
    if not files:
        products = data.get("bulkDataProductBag", [])
        if products:
            file_bag = products[0].get("productFileBag", {})
            files = file_bag.get("fileDataBag", [])

    if not files:
        print("  Response structure:")
        print(f"  Keys: {list(data.keys())}")
        with open(os.path.join(DOWNLOAD_DIR, "api_response.json"), "w") as f:
            json.dump(data, f, indent=2)
        print(f"  Full response saved to {DOWNLOAD_DIR}/api_response.json")
        print("  Check this file to find the download URLs manually.")
        return None

    for f in files:
        name = f.get("fileName", f.get("name", ""))
        url = f.get("fileDownloadURI", f.get("fileDownloadUrl",
              f.get("downloadUrl", f.get("url", ""))))

        # Check if this is one of our needed files
        for needed in NEEDED_FILES:
            if needed in name.lower():
                file_urls[needed] = {"url": url, "filename": name}
                print(f"  Found: {name} ({f.get('fileSize', 0) / 1e6:.0f}MB)")

    return file_urls


# ---------------------------------------------------------------------------
# STEP 1b: Manual fallback — if you already know the download URLs
# ---------------------------------------------------------------------------
def get_file_urls_manual():
    """
    If the API doesn't work or the response structure is unexpected,
    you can set the URLs manually here after finding them on
    data.uspto.gov/bulkdata/datasets/pvgpatdis

    Look for files named something like:
      g_patent.tsv.zip
      g_assignee_disambiguated.tsv.zip
    """
    # FILL THESE IN if the API approach doesn't work:
    return {
        "g_patent": {
            "url": "",  # paste the download URL here
            "filename": "g_patent.tsv.zip"
        },
        "g_assignee_disambiguated": {
            "url": "",  # paste the download URL here
            "filename": "g_assignee_disambiguated.tsv.zip"
        }
    }


# ---------------------------------------------------------------------------
# STEP 2: Download the files
# ---------------------------------------------------------------------------
def download_file(url, dest_path, api_key):
    """Download a file from ODP with API key auth."""
    print(f"  Downloading to {dest_path}...")

    headers = {"x-api-key": api_key}
    resp = requests.get(url, headers=headers, stream=True, timeout=300)

    if resp.status_code != 200:
        print(f"  Download failed: {resp.status_code}")
        return False

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0

    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0:
                pct = (downloaded / total) * 100
                print(f"\r  {downloaded / 1e6:.1f}MB / {total / 1e6:.1f}MB ({pct:.0f}%)", end="")

    print()  # newline after progress
    return True


# ---------------------------------------------------------------------------
# STEP 3: Extract and process
# ---------------------------------------------------------------------------
def extract_tsv(zip_path):
    """Extract a .tsv file from a zip archive. Returns the path to the TSV."""
    print(f"  Extracting {zip_path}...")
    extract_dir = os.path.dirname(zip_path)

    with zipfile.ZipFile(zip_path, "r") as z:
        tsv_files = [f for f in z.namelist() if f.endswith(".tsv")]
        if not tsv_files:
            print(f"  No .tsv files found in archive!")
            return None
        z.extract(tsv_files[0], extract_dir)
        return os.path.join(extract_dir, tsv_files[0])


def build_patent_lookup(patent_tsv, assignee_tsv):
    """
    Join the patent and assignee tables. Filter to recent patents.
    Build a lookup dict: company_name -> {count, titles, dates}.

    This is where the heavy lifting happens, but it only runs quarterly.
    """
    cutoff_date = (datetime.now() - timedelta(days=PATENT_LOOKBACK_YEARS * 365)).strftime("%Y-%m-%d")

    print(f"\nBuilding patent lookup (patents after {cutoff_date})...")

    # STEP A: Load recent patents into a dict (patent_id -> {date, title})
    print("  Loading patent table...")
    patents = {}
    with open(patent_tsv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            patent_date = row.get("patent_date", "")
            if patent_date >= cutoff_date:
                patents[row["patent_id"]] = {
                    "date": patent_date,
                    "title": row.get("patent_title", ""),
                }

    print(f"  Loaded {len(patents):,} recent patents")

    # STEP B: Read assignee table, join to recent patents
    print("  Loading assignee table and joining...")
    company_patents = defaultdict(lambda: {"count": 0, "titles": [], "dates": []})

    with open(assignee_tsv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            patent_id = row.get("patent_id", "")
            if patent_id in patents:
                org = row.get("disambig_assignee_organization", "").strip()
                if org:
                    company_patents[org]["count"] += 1
                    # Keep only first 5 titles to save space
                    if len(company_patents[org]["titles"]) < 5:
                        company_patents[org]["titles"].append(
                            patents[patent_id]["title"]
                        )
                    company_patents[org]["dates"].append(
                        patents[patent_id]["date"]
                    )

    print(f"  Found {len(company_patents):,} unique assignee organizations")

    # STEP C: Clean up — sort dates, keep only count + sample titles
    lookup = {}
    for org, data in company_patents.items():
        lookup[org.lower()] = {
            "organization": org,  # original casing
            "patent_count": data["count"],
            "sample_titles": data["titles"][:5],
            "latest_patent_date": max(data["dates"]) if data["dates"] else "",
        }

    return lookup


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("PATENT DATA DOWNLOADER & PROCESSOR")
    print(f"Lookback: {PATENT_LOOKBACK_YEARS} years")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)

    if not USPTO_API_KEY:
        print("\n[!] No USPTO_API_KEY set.")
        print("    export USPTO_API_KEY='your_key_here'")
        return

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 1: Get download URLs
    file_urls = get_file_urls(USPTO_API_KEY)

    if not file_urls:
        print("\n[!] Couldn't get URLs from API.")
        print("    Go to data.uspto.gov/bulkdata/datasets/pvgpatdis")
        print("    Find g_patent and g_assignee_disambiguated download links")
        print("    Paste them into get_file_urls_manual() in this script")
        file_urls = get_file_urls_manual()
        if not file_urls["g_patent"]["url"]:
            print("\n[!] No manual URLs set either. Exiting.")
            return

    # Step 2: Download
    downloaded_files = {}
    for key, info in file_urls.items():
        dest = os.path.join(DOWNLOAD_DIR, info["filename"])
        if os.path.exists(dest):
            print(f"\n[~] {info['filename']} already exists, skipping download")
            downloaded_files[key] = dest
        else:
            print(f"\nDownloading {info['filename']}...")
            if download_file(info["url"], dest, USPTO_API_KEY):
                downloaded_files[key] = dest
            else:
                print(f"[!] Failed to download {key}")

    if len(downloaded_files) < 2:
        print("\n[!] Missing files. Need both g_patent and g_assignee_disambiguated.")
        return

    # Step 3: Extract
    tsv_files = {}
    for key, zip_path in downloaded_files.items():
        if zip_path.endswith(".zip"):
            tsv_path = extract_tsv(zip_path)
        else:
            tsv_path = zip_path  # already a TSV
        tsv_files[key] = tsv_path

    # Step 4: Build lookup
    lookup = build_patent_lookup(
        tsv_files["g_patent"],
        tsv_files["g_assignee_disambiguated"]
    )

    # Step 5: Save
    output_path = os.path.join(OUTPUT_DIR, "patent_lookup.json")
    with open(output_path, "w") as f:
        json.dump({
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "lookback_years": PATENT_LOOKBACK_YEARS,
            "total_organizations": len(lookup),
            "lookup": lookup,
        }, f)

    # Don't pretty-print — file is big enough already
    file_size = os.path.getsize(output_path) / 1e6
    print(f"\n{'=' * 60}")
    print(f"DONE! Patent lookup saved to {output_path}")
    print(f"File size: {file_size:.1f}MB")
    print(f"Organizations: {len(lookup):,}")
    print(f"\nYou can now delete the raw files in {DOWNLOAD_DIR}/")
    print(f"to free up disk space. Re-run quarterly to update.")


if __name__ == "__main__":
    main()
