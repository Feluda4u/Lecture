
import requests
import sys
import os
import subprocess
import re
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")
BASE_DIR = "CLASSPLUS"
MAX_LINKS = 15
BASE_DIR = "/sdcard/IIT"
MAX_PARTS = 10000
START_PART = 0
STOP_AFTER_MISSES = 3
CONCURRENT_DOWNLOADS = 3

os.makedirs(BASE_DIR, exist_ok=True)

def extract_details(ts_url):
    parsed_url = urlparse(ts_url)
    path_parts = parsed_url.path.rsplit('/', 1)
    filename = path_parts[-1]

    match = re.match(r"(.*?)(\d+)\.ts", filename)
    if match:
        prefix = match.group(1)
        number_len = len(match.group(2))
        base_path = path_parts[0]
        return prefix, base_path, parsed_url, number_len
    else:
        return None, None, None, None

def fetch_part(full_url):
    try:
        res = requests.get(full_url, timeout=10)
        if res.status_code == 200:
            return res.content
        else:
            return None
    except:
        return None

def download_and_merge(link, folder_index, video_index):
    prefix, base_path, parsed_url, num_len = extract_details(link)
    if prefix is None:
        print(f"Invalid URL format: {link}")
        return

    query = parsed_url.query
    output_dir = os.path.join(BASE_DIR, str(folder_index))
    original_output_dir = output_dir
    count = 1
    while os.path.exists(output_dir):
        output_dir = f"{original_output_dir}_{count}"
        count += 1
    os.makedirs(output_dir)

    output_path = os.path.join(output_dir, f"Lecture{video_index}.mp4")

    misses = 0
    parts = []

    print(f"\n[Folder {output_dir}] Downloading parts:")

    with ThreadPoolExecutor(max_workers=CONCURRENT_DOWNLOADS) as executor:
        i = START_PART
        while i < MAX_PARTS:
            futures = {}
            for j in range(CONCURRENT_DOWNLOADS):
                part_name = f"{prefix}{i+j:0{num_len}d}.ts"
                full_url = f"{parsed_url.scheme}://{parsed_url.netloc}{base_path}/{part_name}"
                if query:
                    full_url += f"?{query}"
                futures[executor.submit(fetch_part, full_url)] = i + j

            success = False
            for future in as_completed(futures):
                idx = futures[future]
                data = future.result()
                if data:
                    parts.append((idx, data))
                    print(f"Downloaded: {prefix}{idx:0{num_len}d}.ts")
                    misses = 0
                    success = True
                else:
                    print(f"Missing: {prefix}{idx:0{num_len}d}.ts")
                    misses += 1

            if not success and misses >= STOP_AFTER_MISSES:
                print(f"Stopped after {STOP_AFTER_MISSES} consecutive missing parts.")
                break

            i += CONCURRENT_DOWNLOADS

    if parts:
        parts.sort()  # Ensure correct order
        print("Merging with FFmpeg...")

        ffmpeg = subprocess.Popen(
            ["ffmpeg", "-f", "mpegts", "-i", "pipe:0", "-c", "copy", "-threads", "8",
             "-preset", "ultrafast", output_path],
            stdin=subprocess.PIPE
        )

        for _, segment in parts:
            ffmpeg.stdin.write(segment)
        ffmpeg.stdin.close()
        ffmpeg.wait()

        print(f"Merged to: {output_path}")
    else:
        print("No parts downloaded to merge.")

def main():
    if len(sys.argv) < 3:
        print("Usage: python i.py <start_index> <link1> [<link2> ... up to 15 links]")
        return

    try:
        start_index = int(sys.argv[1])
    except ValueError:
        print("Start index must be an integer.")
        return

    links = sys.argv[2:2 + MAX_LINKS]
    for idx, link in enumerate(links, 1):
        download_and_merge(link, idx, start_index + idx - 1)

if __name__ == "__main__":
    main()
