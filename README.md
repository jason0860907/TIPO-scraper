# TIPO Patent Data Scraper & Metadata Generator

This project provides tools to automate the download of patent data from the Taiwan Intellectual Property Office (TIPO) and to generate structured metadata from the downloaded XML files.

## Overview

- **scraper.py**: Automates the process of selecting a year on the TIPO website, extracting FTPS download links, and mirroring the patent data using `lftp`.
- **generate_metadata.py**: Parses downloaded XML files and generates CSV metadata for each dataset, including image and classification information.

---

## Requirements

- Python 3.8+
- Google Chrome browser
- [lftp](https://lftp.yar.ru/) (for FTPS downloads)

Install dependencies with:

```bash
uv venv -p 3.12 --seed
source .venv/bin/activate
uv sync
```

---

## Usage

### 1. Download Patent Data

Use `scraper.py` to download patent data for a specific year.

```bash
python scraper.py --year 114
```

- Downloads all FTPS patent data for the specified year (e.g., 114).
- Data is saved in a folder named `<year>` (e.g., `114`).

**Note:**  
- `lftp` must be installed and available in your system.

### 2. Generate Metadata

After downloading, run `generate_metadata.py` to parse XML files and generate CSV metadata.

```bash
python generate_metadata.py --root example
```

- By default, processes all folders under specified root folder (e.g., `example/`).
- For each dataset, creates a CSV file (e.g., `<datasetname>_metadata.csv`) with metadata for images and classification codes.
