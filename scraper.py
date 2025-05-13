import argparse
import os
import time
import subprocess
import concurrent.futures
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from loguru import logger


def get_ftps_links(selected_year, download_root):
    # === Configuration ===
    url = "https://cloud.tipo.gov.tw/S220/opdata/detail/PatentPubXML"
    os.makedirs(download_root, exist_ok=True)

    # === Setup Selenium ===
    logger.info("Launching browser...")
    options = Options()
    options.add_argument("--headless")  # Runs browser in background
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)

    logger.info(f"Opening {url}")
    driver.get(url)
    time.sleep(3)
    # Find the dropdown element (usually a <select> tag)
    dropdown_element = driver.find_element(By.XPATH, '//*[@id="root"]//select')  # or use another locator
    # Create a Select object
    select = Select(dropdown_element)
    select.select_by_value(selected_year)
    logger.info(f"Selected year: {selected_year}")
    time.sleep(3)
    html = driver.page_source
    driver.quit()

    # === Parse HTML ===
    soup = BeautifulSoup(html, "html.parser")
    ftps_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].startswith("ftps://")]
    logger.info(f"Found {len(ftps_links)} FTPS links")
    for link in ftps_links:
        logger.info(f"Mirroring: {link}")
    return ftps_links


# === lftp mirror for folder-preserving downloads ===
def mirror_with_lftp(ftps_url, download_root):
    parsed = urlparse(ftps_url)
    host = parsed.hostname
    remote_path = parsed.path

    # Define a timeout in seconds (e.g., 1 hour = 3600 seconds)
    TIMEOUT_SECONDS = 10000

    # We'll mirror into local path: downloads/selected_year
    local_mirror_path = os.path.join(download_root, remote_path.split('/')[-1])
    os.makedirs(local_mirror_path, exist_ok=True)

    logger.info(f"Mirroring: {remote_path}")
    cmd = [
        "lftp", "-e",
        f"set ssl:check-hostname no; open ftps://{host}; mirror --use-pget-n=4 --verbose {remote_path} .; bye"
    ]

    try:
        result = subprocess.run(
            cmd,
            cwd=local_mirror_path,
            capture_output=True,
            text=True,
            check=True,
            timeout=TIMEOUT_SECONDS
        )
        logger.success(f"Mirrored: {remote_path}")
        # Optionally log the full output if needed
        logger.debug(f"lftp stdout: {result.stdout.strip()}")
        return (remote_path, "Success", result.stdout.strip()) # Keep return for potential other uses
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to mirror: {remote_path}")
        logger.error(f"lftp stderr:\n{e.stderr}")
        return (remote_path, "Failed", e.stderr.strip()) # Keep return
    except subprocess.TimeoutExpired as e:
        logger.error(f"Timeout ({TIMEOUT_SECONDS}s) expired while mirroring: {remote_path}")
        stderr_output = e.stderr.strip() if e.stderr else "No stderr captured before timeout."
        logger.error(f"lftp stderr before timeout:\n{stderr_output}")
        return (remote_path, "Timeout", stderr_output)


if __name__ == "__main__":
    # === Parse CLI arguments ===
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=str, default='114', required=True, help="Year to select on the TIPO site (e.g. 114)")
    args = parser.parse_args()
    selected_year = args.year
    download_root = selected_year

    # === Configure Loguru ===
    log_file_path = os.path.join(f"{selected_year}-scraper.log")
    # Ensure download_root exists before setting up the logger
    os.makedirs(download_root, exist_ok=True)
    logger.add(log_file_path, rotation="10 MB", level="INFO") # Configure file logging
    logger.info(f"Logging to {log_file_path}")

    ftps_links = get_ftps_links(selected_year, download_root)
    
    successful_mirrors = 0
    failed_mirrors = 0
    timeout_mirrors = 0 # Add a counter for timeouts

    # Define the maximum number of concurrent threads
    MAX_WORKERS = 11 

    # === Loop through all links and mirror each using ThreadPoolExecutor ===
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all download tasks to the executor
        future_to_link = {executor.submit(mirror_with_lftp, link, download_root): link for link in ftps_links}

        for future in concurrent.futures.as_completed(future_to_link):
            link = future_to_link[future]
            try:
                _, status, _ = future.result() # Unpack result
                if status == "Success":
                    successful_mirrors += 1
                elif status == "Timeout":
                    timeout_mirrors += 1 # Increment timeout counter
                    failed_mirrors += 1 # Also count timeouts as failures for the summary
                else:
                    failed_mirrors += 1
            except Exception as exc:
                logger.error(f'{link} generated an exception: {exc}')
                failed_mirrors += 1

    # === Log summary ===
    logger.info("Download process complete.")
    logger.info(f"Total links processed: {len(ftps_links)}")
    logger.info(f"Successful mirrors: {successful_mirrors}")
    logger.info(f"Failed mirrors: {failed_mirrors}")
    if timeout_mirrors > 0:
        logger.info(f"Mirrors that timed out: {timeout_mirrors}")
    logger.info(f"Detailed logs saved to {log_file_path}")
