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
    url = "https://cloud.tipo.gov.tw/S220/opdata/detail/PatentIsuRegSpecXMLA"
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


# === Helper function to get remote directory count ===
def get_remote_directory_count(host, remote_path_on_host, ftps_url_for_logging):
    logger.info(f"Phase 1: Fetching remote directory count for: {ftps_url_for_logging} (Path: {remote_path_on_host})")
    remote_dir_count = -1
    cmd_list_items = [
        "lftp", "-e",
        f"set ssl:check-hostname no; open ftps://{host}; cls -1 {remote_path_on_host}; bye"
    ]

    try:
        list_result = subprocess.run(
            cmd_list_items,
            capture_output=True,
            text=True,
            check=True,
            timeout=120  # Timeout for listing
        )
        raw_cls_output = list_result.stdout.strip()
        logger.debug(f"Phase 1: Raw cls -1 output for {remote_path_on_host}:\n{raw_cls_output}")

        remote_directories = [line for line in raw_cls_output.split('\n') if line and line.endswith('/')]
        remote_dir_count = len(remote_directories)
        logger.info(f"Phase 1: Remote path {remote_path_on_host} contains {remote_dir_count} directories based on parsed output.")
        return remote_dir_count
    except subprocess.CalledProcessError as e:
        logger.error(f"Phase 1: Failed to list items in {remote_path_on_host}: {e.stderr.strip()}")
        return -1
    except subprocess.TimeoutExpired:
        logger.error(f"Phase 1: Timeout while listing items in {remote_path_on_host}")
        return -1
    except Exception as e:
        logger.error(f"Phase 1: An unexpected error occurred while listing items in {remote_path_on_host}: {e}")
        return -1

# === Function for Phase 2: Mirroring and Local Verification ===
def mirror_and_verify_link(ftps_url, download_root, expected_remote_dir_count):
    parsed = urlparse(ftps_url)
    host = parsed.hostname
    remote_path = parsed.path

    MIRROR_TIMEOUT_SECONDS = 10000
    local_target_dir_name = remote_path.split('/')[-1]
    if not local_target_dir_name and remote_path == "/":
        local_target_dir_name = "ftps_root"
    elif not local_target_dir_name:
        segments = [s for s in remote_path.split('/') if s]
        if segments:
            local_target_dir_name = segments[-1]
        else:
            local_target_dir_name = "unknown_target_dir"
            logger.warning(f"Phase 2: Could not determine target directory name for {ftps_url}, using '{local_target_dir_name}'")

    local_mirror_destination_path = os.path.join(download_root, local_target_dir_name)
    os.makedirs(local_mirror_destination_path, exist_ok=True)

    logger.info(f"Phase 2: Attempting to mirror: {ftps_url} into {local_mirror_destination_path}")
    cmd_mirror = [
        "lftp", "-e",
        f"set ssl:check-hostname no; open ftps://{host}; mirror --use-pget-n=4 --only-newer --continue --verbose {remote_path} .; bye"
    ]

    lftp_mirror_output = ""
    mirror_operation_status = "Unknown"
    local_dir_count = 0 # Initialize local_dir_count

    try:
        result = subprocess.run(
            cmd_mirror,
            cwd=local_mirror_destination_path,
            capture_output=True,
            text=True,
            check=True,
            timeout=MIRROR_TIMEOUT_SECONDS
        )
        logger.success(f"Phase 2: Successfully mirrored: {ftps_url} to {local_mirror_destination_path}")
        lftp_mirror_output = result.stdout.strip()
        mirror_operation_status = "Success"

        for r, dirs, files in os.walk(local_mirror_destination_path):
            if r == local_mirror_destination_path:
                local_dir_count = len(dirs)
                break
        logger.info(f"Phase 2: Local path {local_mirror_destination_path} (for {ftps_url}) contains {local_dir_count} sub-directories.")

        if expected_remote_dir_count != -1:
            if local_dir_count != expected_remote_dir_count:
                logger.warning(f"Phase 2: Directory count mismatch for {ftps_url}: Remote Expected={expected_remote_dir_count}, Local Actual={local_dir_count}")
            else:
                logger.success(f"Phase 2: Directory count matches for {ftps_url}: {local_dir_count} directories.")
        else:
            logger.warning(f"Phase 2: Remote directory count was not obtained for {ftps_url}. Local count: {local_dir_count}. Mirror: {mirror_operation_status}")
        
        return (ftps_url, mirror_operation_status, lftp_mirror_output, expected_remote_dir_count, local_dir_count)

    except subprocess.CalledProcessError as e:
        logger.error(f"Phase 2: Failed to mirror {ftps_url}: {e.stderr.strip()}")
        mirror_operation_status = "Failed"
        lftp_mirror_output = e.stderr.strip()
    except subprocess.TimeoutExpired as e:
        logger.error(f"Phase 2: Timeout ({MIRROR_TIMEOUT_SECONDS}s) expired while mirroring {ftps_url}")
        mirror_operation_status = "Timeout"
        lftp_mirror_output = e.stderr.strip() if e.stderr else "No stderr captured before timeout."
    except Exception as e:
        logger.error(f"Phase 2: An unexpected error occurred during mirroring/verification of {ftps_url}: {e}")
        mirror_operation_status = "Error"
        lftp_mirror_output = str(e)
    
    # Return outside the try block to ensure it always returns, even on handled exceptions
    return (ftps_url, mirror_operation_status, lftp_mirror_output, expected_remote_dir_count, local_dir_count)


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
    if not ftps_links:
        logger.warning("No FTPS links found. Exiting.")
        exit()

    MAX_WORKERS = 8
    remote_counts_data = {} # To store {ftps_url: remote_dir_count}

    # === Phase 1: Get all remote directory counts ===
    logger.info("Starting Phase 1: Fetching all remote directory counts...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='CountWorker') as executor:
        future_to_url_count = {}
        for link in ftps_links:
            parsed = urlparse(link)
            future = executor.submit(get_remote_directory_count, parsed.hostname, parsed.path, link)
            future_to_url_count[future] = link
        
        for future in concurrent.futures.as_completed(future_to_url_count):
            link = future_to_url_count[future]
            try:
                count = future.result()
                remote_counts_data[link] = count
            except Exception as exc:
                logger.error(f"Phase 1: Exception for {link} while getting remote count: {exc}")
                remote_counts_data[link] = -1 # Mark as failed
    logger.info("Phase 1: Completed fetching remote directory counts.")
    for link, count in remote_counts_data.items():
        if count == -1:
            logger.warning(f"Phase 1 Result: Failed to get remote count for {link}")
        else:
            logger.info(f"Phase 1 Result: {link} has {count} remote directories.")

    successful_mirrors = 0
    failed_mirrors = 0
    timeout_mirrors = 0

    # === Phase 2: Mirroring and Local Verification ===
    logger.info("Starting Phase 2: Mirroring links and verifying directory counts...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='MirrorWorker') as executor:
        future_to_url_mirror = {}
        for link in ftps_links:
            expected_count = remote_counts_data.get(link, -1) # Get pre-fetched count
            if expected_count == -1:
                logger.warning(f"Phase 2: Skipping mirror for {link} as remote count was not obtained.")
                # Optionally, count this as a failure straight away if desired
                # failed_mirrors += 1 
                continue # Skip submitting this link for mirroring

            future = executor.submit(mirror_and_verify_link, link, download_root, expected_count)
            future_to_url_mirror[future] = link

        for future in concurrent.futures.as_completed(future_to_url_mirror):
            original_link = future_to_url_mirror[future]
            try:
                processed_url, status, lftp_out, remote_c, local_c = future.result()
                
                if status == "Success":
                    successful_mirrors += 1
                    # Comparison logging is already detailed in mirror_and_verify_link
                elif status == "Timeout":
                    timeout_mirrors += 1
                    failed_mirrors += 1
                else: # Failed or Error
                    failed_mirrors += 1
            except Exception as exc:
                logger.error(f"Phase 2: Task for {original_link} generated an exception: {exc}")
                failed_mirrors += 1

    # === Log summary ===
    logger.info("Download process complete.")
    logger.info(f"Total links processed: {len(ftps_links)}")
    logger.info(f"Successful mirrors: {successful_mirrors}")
    logger.info(f"Failed mirrors: {failed_mirrors}")
    if timeout_mirrors > 0:
        logger.info(f"Mirrors that timed out: {timeout_mirrors}")
    logger.info(f"Detailed logs saved to {log_file_path}")

