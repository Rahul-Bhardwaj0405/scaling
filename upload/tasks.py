import pandas as pd
import logging
from io import StringIO, BytesIO
from .models import BookingData, RefundData
from celery import shared_task
from django.db import connection
import json
from pyexcel_ods import get_data as ods_get_data


# For Oracle DB connection
# import cx_Oracle

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Bank name to code mapping (can be stored in DB)
BANK_CODE_MAPPING = {
    'hdfc': 101,
    'icici': 102,
    'karur_vysya': 40,
}


# Bank-specific mappings for booking and refund files
BANK_MAPPINGS = {
    'hdfc': {
        'booking': {
            'columns': ['IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT'],
            'column_mapping': {
                'IRCTC ORDER NO.': 'irctc_order_no',
                'BANK BOOKING REF.NO.': 'bank_booking_ref_no',
                'BOOKING AMOUNT': 'sale_amount'
            }
        },
        'refund': {
            'columns': ['REFUND ORDER NO.', 'REFUND AMOUNT', 'CREDITED ON'],
            'column_mapping': {
                'REFUND ORDER NO.': 'irctc_order_no',
                'REFUND AMOUNT': 'sale_amount',
                'CREDITED ON': 'date'
            }
        }
    },
    'icici': {
        'booking': {
            'columns': ['ORDER NO.', 'REFERENCE NO.', 'AMOUNT'],
            'column_mapping': {
                'ORDER NO.': 'irctc_order_no',
                'REFERENCE NO.': 'bank_booking_ref_no',
                'AMOUNT': 'sale_amount',
                'BRANCH CODE': 'branch_code',
                'OTHER COLUMN': 'other_column'
            }
        },
        'refund': {
            'columns': ['ORDER NO.', 'REFUND AMOUNT', 'CREDIT DATE'],
            'column_mapping': {
                'ORDER NO.': 'irctc_order_no',
                'REFUND AMOUNT': 'sale_amount',
                'CREDIT DATE': 'date',
                'BRANCH CODE': 'branch_code',
                'OTHER COLUMN': 'other_column'
            }
        }
    },

    'karur_vysya': {
        'booking': {
            'columns': ['txn_date', 'irctc_order_no', 'bank_booking_ref_no', 'booking_amount', 'credited_on'],
            'column_mapping': {
                'txn_date': 'TXN DATE',
                'irctc_order_no': 'IRCTC ORDER NO.',
                'bank_booking_ref_no': 'BANK BOOKING REF.NO.',
                'booking_amount': 'BOOKING AMOUNT',
                'credited_on': 'CREDITED ON'
            }
        },

        'refund': {
            'columns': ['refund_date', 'refund_amount', 'debited_on_date', 'irctc_order_no', 'bank_booking_ref_no', 'bank_refund_ref_no'],
            'column_mapping': {
                'refund_date': 'REFUND DATE',
                'refund_amount': 'REFUND AMOUNT',
                'debited_on_date': 'DEBITED ON',
                'irctc_order_no': 'IRCTC ORDER NO.',
                'bank_booking_ref_no': 'BANK BOOKING REF.NO.',
                'bank_refund_ref_no': 'BANK REFUND REF.NO.'
    }
}
    },
    # Add more bank mappings as needed
}

@shared_task
def process_uploaded_files(file_content, file_name, bank_name, transaction_type):
    logging.info(f"Starting to process file: {file_name} for bank: {bank_name}, transaction type: {transaction_type}")

    try:
        df = pd.DataFrame()

        # Set possible delimiters for CSV and text files
        possible_delimiters = [',', ';', '\t', '|', ' ', '.', '_']

        if file_name.endswith('.csv') or file_name.endswith('.txt'):
            file_str = file_content.decode(errors='ignore')
            delimiter = next((delim for delim in possible_delimiters if delim in file_str), ',')
            df = pd.read_csv(StringIO(file_str), delimiter=delimiter, dtype=str)
            logging.info(f"CSV/TXT file read successfully with delimiter '{delimiter}'.")

        elif file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(BytesIO(file_content), engine='openpyxl', dtype={
                'IRCTCORDERNO': str,
                'BANKBOOKINGREFNO': str,
                'BANKREFUNDREFNO': str
            })
            logging.info(f"Excel file read successfully: {file_name}.")

        else:
            logging.info(f"Unsupported file type {file_name}. Converting to CSV.")
            file_str = convert_to_csv(BytesIO(file_content), file_name)  # Function to convert other formats to CSV
            df = pd.read_csv(StringIO(file_str), delimiter=',', dtype=str)

                
        # Clean and process data
        df.columns = df.columns.str.strip()  # Remove leading and trailing spaces
        df.columns = df.columns.str.replace(r'\W+', '', regex=True)  # Remove non-word characters
        logging.info(f"Columns after cleaning: {df.columns.tolist()}")


        # Get the specific mappings for the bank and transaction type
        mappings = BANK_MAPPINGS.get(bank_name, {}).get(transaction_type)

        if not mappings:
            raise ValueError(f"No mapping found for bank: {bank_name}, transaction type: {transaction_type}")

        # Check for missing columns
        required_columns = mappings['columns']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame: {missing_columns}")

        # Filter columns and rename them based on the mapping
        df = df[mappings['columns']]
        # df.rename(columns=mappings['column_mapping'], inplace=True)

        if transaction_type == 'booking':
            # Convert numeric fields for booking
            df['IRCTCORDERNO'] = df['IRCTCORDERNO'].apply(lambda x: int(float(x)) if pd.notnull(x) else 0)
            df['BANKBOOKINGREFNO'] = df['BANKBOOKINGREFNO'].apply(lambda x: int(float(x)) if pd.notnull(x) else 0)
            df['BOOKINGAMOUNT'] = pd.to_numeric(df['BOOKINGAMOUNT'], errors='coerce')


            # Convert date formats
            df['TXNDATE'] = pd.to_datetime(df['TXNDATE'], format='%d-%b-%y', errors='coerce')
            df['CREDITEDON'] = pd.to_datetime(df['CREDITEDON'], format='%d-%b-%y', errors='coerce')

            # Check for any NaT (Not a Time) values in the date columns after conversion
            if df['TXNDATE'].isnull().any() or df['CREDITEDON'].isnull().any():
                invalid_dates = df[df['TXNDATE'].isnull() | df['CREDITEDON'].isnull()]
                logging.error(f"Invalid date formats found in booking data: {invalid_dates[['TXNDATE', 'CREDITEDON']]}")
                return  # Exit the function if invalid dates are found

            # Add logic to handle booking data
            bank_code = BANK_CODE_MAPPING.get(bank_name)
            if not bank_code:
                raise ValueError(f"No bank code found for bank: {bank_name}")

            for _, row in df.iterrows():
                # Check for duplicate booking data
                booking_exists = BookingData.objects.filter(
                    irctc_order_no=row['IRCTCORDERNO'],
                    bank_booking_ref_no=row['BANKBOOKINGREFNO']
                ).exists()

                if booking_exists:
                    logging.info(f"Duplicate booking found: IRCTCORDERNO {row['IRCTCORDERNO']} - BANKBOOKINGREFNO {row['BANKBOOKINGREFNO']}. Skipping...")
                else:
                    BookingData.objects.create(
                        bank_code = bank_code,
                        txn_date=row['TXNDATE'],  
                        credited_on_date=row['CREDITEDON'],  
                        booking_amount=row['BOOKINGAMOUNT'],
                        irctc_order_no=row['IRCTCORDERNO'],
                        bank_booking_ref_no=row['BANKBOOKINGREFNO']
                        # Add other fields as needed based on your model structure
                        # branch_code=row['branch_code'],  # Extracted from DataFrame
                        # other_column=row['other_column'],  # Extracted from DataFrame
                    )
                    logging.info(f"Booking data saved for IRCTCORDERNO {row['irctc_order_no']}.")

        elif transaction_type == 'refund':
            # Convert numeric fields for refund
            df['IRCTCORDERNO'] = df['IRCTCORDERNO'].apply(lambda x: int(float(x)) if pd.notnull(x) else 0)
            df['BANKBOOKINGREFNO'] = df['BANKBOOKINGREFNO'].apply(lambda x: int(float(x)) if pd.notnull(x) else 0)
            df['BANKREFUNDREFNO'] = df['BANKREFUNDREFNO'].apply(lambda x: int(float(x)) if pd.notnull(x) else 0)
            df['REFUNDAMOUNT'] = pd.to_numeric(df['REFUNDAMOUNT'], errors='coerce')

            
            # Convert date formats
            df['REFUNDDATE'] = pd.to_datetime(df['REFUNDDATE'], format='%d-%b-%y', errors='coerce')
            df['DEBITEDON'] = pd.to_datetime(df['DEBITEDON'], format='%d-%b-%y', errors='coerce')

            # Check for any NaT (Not a Time) values in the date columns after conversion
            if df['REFUNDDATE'].isnull().any() or df['DEBITEDON'].isnull().any():
                invalid_dates = df[df['REFUNDDATE'].isnull() | df['DEBITEDON'].isnull()]
                logging.error(f"Invalid date formats found in booking data: {invalid_dates[['REFUNDDATE', 'DEBITEDON']]}")
                return  # Exit the function if invalid dates are found




            # Add logic to handle refund data
            bank_code = BANK_CODE_MAPPING.get(bank_name)
            if not bank_code:
                raise ValueError(f"No bank code found for bank: {bank_name}")

            for _, row in df.iterrows():
                # Check for duplicate refund data
                refund_exists = RefundData.objects.filter(
                    irctc_order_no=row['IRCTCORDERNO'],
                    bank_refund_ref_no=row['BANKREFUNDREFNO']
                ).exists()

                if refund_exists:
                    logging.info(f"Duplicate refund found: IRCTCORDERNO {row['IRCTCORDERNO']} - BANKREFUNDREFNO {row['BANKREFUNDREFNO']}. Skipping...")
                else:
                    RefundData.objects.create(
                        bank_code = bank_code,
                        refund_date=row['REFUNDDATE'],  # Set as needed
                        debited_on_date=row['DEBITEDON'],  # Set as needed
                        refund_amount=row['REFUNDAMOUNT'],
                        irctc_order_no=row['IRCTCORDERNO'],
                        bank_booking_ref_no=row['BANKBOOKINGREFNO'],
                        bank_refund_ref_no=row['BANKREFUNDREFNO']
                        # Add other fields as needed based on your model structure
                        # branch_code=row['branch_code'],  # Extracted from DataFrame
                        # other_column=row['other_column'],  # Extracted from DataFrame
                    )
                    logging.info(f"Refund data saved for IRCTCORDERNO {row['irctc_order_no']}.")

    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")

# # Function to compare development and production DB data
# def compare_db_data(bank_name, year, month):
#     unmatched_records = []
#     # Fetch development data
#     development_data = BookingData.objects.filter(bank_name=bank_name, year=year, month=month)

#     # Fetch production data from production Oracle DB
#     with cx_Oracle.connect('user/password@production_db') as prod_conn:
#         prod_cursor = prod_conn.cursor()
#         prod_cursor.execute("SELECT * FROM production_booking_data WHERE bank_code=:bank_code", {'bank_code': BANK_CODE_MAPPING[bank_name]})
#         production_data = prod_cursor.fetchall()

#         # Compare and find unmatched records
#         for prod_row in production_data:
#             if not development_data.filter(irctc_order_no=prod_row[0]).exists():
#                 unmatched_records.append(prod_row)

#     return unmatched_records

# Function to convert non-CSV/Excel files to CSV
def convert_to_csv(file_content, file_name):
    # Implement conversion logic based on file type
    try:
        file_content.seek(0)  # Reset file pointer to the start
        content = file_content.read()

        # Handle ODS (OpenDocument Spreadsheet) files
        if file_name.endswith('.ods'):
            logging.info("Converting ODS file to CSV.")
            data = ods_get_data(file_content)
            # Assuming the first sheet contains the data you need
            sheet_data = data[next(iter(data))]
            df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])  # First row as header
            return df.to_csv(index=False)

        # Handle JSON files
        elif file_name.endswith('.json'):
            logging.info("Converting JSON file to CSV.")
            json_data = json.loads(content)
            df = pd.json_normalize(json_data)  # Flatten JSON if needed
            return df.to_csv(index=False)

        else:
            raise ValueError(f"Unsupported file format: {file_name}")

    except Exception as e:
        logging.error(f"Error converting file to CSV: {e}")
        return ""
