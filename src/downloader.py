import logging
import requests

logger = logging.getLogger("SpendingReportDownloader")


class SpendingReportDownloader:
    def __init__(self):
        # sample URL https://cg.sc.gov/sites/cg/files/Documents/Fiscal%20Transparency/Monthly%20Charge%20Card%20Usage/2025/CCU-January2025.pdf
        self._url_template = "https://cg.sc.gov/sites/cg/files/Documents/Fiscal%20Transparency/Monthly%20Charge%20Card%20Usage/{year}/CCU-{month}{year}.pdf"

    def download_report(self, month, year, output_path=None):
        if not output_path:
            output_path = f"{year}-{month}.pdf".lower()
        url = self._url_template.format(month=month, year=year)
        response = requests.get(url)
        with open(output_path, "wb") as f:
            f.write(response.content)
        logger.info(f"Report downloaded and saved to {output_path}")
