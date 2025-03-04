import pdfplumber
import csv
import logging
import sqlite3
from tqdm import tqdm

logger = logging.getLogger("SpendingDataExtractor")
logger.setLevel(logging.ERROR)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(handler)

# file handler
file_handler = logging.FileHandler("extractor.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


class SpendingDataExtractor:
    def extract_data(self, pdf_path):
        logger.info(f"Extracting data from PDF {pdf_path}")
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                logger.info(f"Processing page {page.page_number}")
                for j, table in enumerate(page.extract_tables()):
                    logger.info(f"Processing table {j}")
                    yield from (
                        [cell.strip() if cell else "" for cell in row] for row in table
                    )
        logger.info(f"Data extracted from PDF {pdf_path}")

    def _is_empty(self, row):
        return all(cell == "" for cell in row)

    def _is_company_name(self, row):
        return len(row) == 1 or row[1:] == ["", "", ""] or len(row) == 2

    def _is_page_number(self, row):
        return row[0].replace("/", "").replace(",", "").isdigit()

    def _is_page_title(self, row):
        return row[0] == "State Government Credit Card Usage Report"

    def _is_company_total_row(self, row):
        return "Total for" in row[0]

    def _is_person_total_row(self, row):
        return (
            row[:3] == ["", "", ""]
            and row[3].replace("$", "").replace(",", "").replace(".", "").isdigit()
        )

    def _is_table_header(self, row):
        return row == ["Card Holder", "Vendor Name", "Purchase Date", "Amount"]

    def _dollar_amount_to_float(self, amount):
        return float(amount.replace("$", "").replace(",", ""))

    def _get_name_from_line(self, line):
        # ABC DEF COMPANY 01/09/2025... -> ABC DEF
        return " ".join(line.split()[:2])

    def save_to_csv_file(self, rows, csv_output):
        logger.info(f"Saving data to CSV file {csv_output}")
        with open(csv_output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def save_to_sqlite(self, data, db_name, table_name):
        """
        Args:
            data: A list of dictionaries.
            db_name: The name of the SQLite database file.
            table_name: The name of the table to create and insert data into.
        """
        logger.info(f"Saving data to SQLite database {db_name}")
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        if data:
            # Create table dynamically based on the keys of the first dictionary
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            for key in data[0].keys():
                create_table_sql += (
                    f"{key} TEXT, "  # Assuming all values are text for simplicity
                )
            create_table_sql = create_table_sql.rstrip(", ") + ")"
            cursor.execute(create_table_sql)

            # Insert data
            for row in data:
                placeholders = ", ".join(["?"] * len(row))
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                cursor.execute(insert_sql, list(row.values()))

            conn.commit()
        conn.close()

    def extract_and_save(self, pdf_path, csv_output=None, sqlite_output=None):
        if not csv_output and not sqlite_output:
            raise ValueError("At least one output should be provided")
        if csv_output and sqlite_output:
            raise ValueError("Only one output should be provided")

        rows = []
        # Track the last encountered values
        company_name = ""
        cardholder = ""
        is_person_total = False
        is_empty_row = False
        is_page_title = False
        for i, row in tqdm(
            enumerate(self.extract_data(pdf_path=pdf_path)), desc="Extracting data"
        ):
            i += 1
            logger.info(i)
            try:

                if self._is_empty(row):
                    logger.info({"EMPTY_ROW": row})
                    is_person_total = False
                    is_empty_row = True
                    is_page_title = False
                    continue
                if self._is_page_title(row):
                    logger.info({"PAGE_TITLE": row})
                    is_person_total = False
                    is_empty_row = False
                    is_page_title = True
                    continue
                elif self._is_page_number(row):
                    logger.info({"PAGE_NUMBER": row})
                    is_person_total = False
                    is_empty_row = False
                    is_page_title = False
                    continue
                elif self._is_table_header(row):
                    logger.info({"TABLE_HEADER": row})
                    is_person_total = False
                    is_empty_row = False
                    is_page_title = False
                    continue
                elif self._is_company_total_row(row):
                    logger.info({"COMPANY_TOTAL": row})
                    is_person_total = False
                    is_empty_row = False
                    is_page_title = False
                    continue
                elif self._is_person_total_row(row):
                    logger.info({"PERSON_TOTAL": row})
                    is_person_total = True
                    is_empty_row = False
                    is_page_title = False
                    continue
                elif len(row[0].split("\n")) > 1:
                    logger.info({"MULTI_LINE_CELL": row})
                    _split = row[0].split("\n")
                    _first_line = _split[0]
                    # if split first cell is purely capitalized letters and spaces, it's a company title
                    # if last line was a person's total, this new line is a new person
                    if is_person_total:
                        cardholder = self._get_name_from_line(_first_line)
                        logger.info({"CARDHOLDER_FROM_MULTILINE": cardholder})

                    # if last line was empty or page title, this new line is a company
                    if is_empty_row or is_page_title:
                        company_name = _first_line
                        logger.info({"COMPANY_NAME_FROM_MULTILINE": company_name})

                    is_empty_row = False
                    is_person_total = False
                    is_page_title = False
                    continue
                if (
                    len(row) == 4
                    and row[1:] == ["", "", ""]
                    and "$" in row[0]
                    and row[0].isupper()
                ):
                    # if row looks like ['HOLLY PARK BROADCAST MUSIC INC BMI 01/06/2025 $767.30', '', '', ''] then skip, meaning
                    # if it has ALL UPPERCASE TEXT DATE $DOLLAR_AMOUNT in first cell and rest are empty then pull cardholder name
                    # but skip the row
                    cardholder = self._get_name_from_line(row[0])
                    logger.info({"CARDHOLDER_FROM_DOLLAR_AMOUNT": cardholder})
                    is_empty_row = False
                    is_person_total = False
                    is_page_title = False
                    continue
                # if there is only one cell in the row or if only first row is populated
                elif self._is_company_name(row):
                    logger.info({"COMPANY_NAME": row})
                    company_name = row[0]
                else:
                    logger.info({"CARDHOLDER_SPEND": row})
                    if row[0].strip() != "":
                        cardholder = row[0]
                    data = {
                        "company": company_name,
                        "cardholder": cardholder,
                        "vendor": row[1],
                        "date": row[2],
                        "amount": self._dollar_amount_to_float(row[-1]),
                    }
                    rows.append(data)
            except Exception as e:
                logger.error(f"Error processing row {row}: {e}")
        if csv_output:
            self.save_to_csv_file(rows, csv_output)
        elif sqlite_output:
            self.save_to_sqlite(rows, sqlite_output, "spending_data")

        logger.info(f"Data extracted and saved to {csv_output}")
