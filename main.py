import os
from src import SpendingReportDownloader, SpendingDataExtractor


def main():
    downloader = SpendingReportDownloader()
    extractor = SpendingDataExtractor()
    year, month = 2025, "January"
    pdf_path = f"{year}-{month}.pdf"
    csv_path = f"{year}-{month}.csv"
    if not os.path.exists(pdf_path):
        downloader.download_report(month=month, year=year)

    extractor.extract_and_save(pdf_path=pdf_path, csv_output=csv_path)


if __name__ == "__main__":
    main()
