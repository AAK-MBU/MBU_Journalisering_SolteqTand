"""MISSING"""
import os
import json
import pyodbc
import time
from mbu_dev_shared_components.os2forms.documents import download_file_bytes
from mbu_dev_shared_components.solteqtand.app_handler import SolteqTandApp, PatientOpenError
from mbu_dev_shared_components.solteqtand.db_handler import SolteqTandDatabase
from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure

from robot_framework import config


class ManualProcessingRequiredError(Exception):
    """Class to handle errors that needs to be handled manually."""


def get_next_form(connection_string, table_name):
    """Get form data from Hub in SQL database."""
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT  uuid,
                    JSON_VALUE(data, '$.data.cpr_nummer_barn') as cpr,
                    JSON_VALUE(data, '$.data.jeg_giver_tilladelse_til_at_tandplejen_aarhus_maa_sende_journal_') as samtykke_til_journaloverdragelse,
                    JSON_VALUE(data, '$.data.adresse') as klinik_adresse,
                    JSON_VALUE(data, '$.data.tandklinik') as klinik_navn,
                    JSON_VALUE(data, '$.data.attachments.kvittering_valg_af_privat_tandklinik_som_leverandoer_af_det_komm.url') as url
            FROM    [RPA].[rpa].{table_name}
            WHERE   uuid = '1A76EB35-B824-40AF-B0FA-59F76D8CCBF6'
            """
        )
        rows = cursor.fetchall()
        result = []
        columns = [column[0] for column in cursor.description]

        for row in rows:
            result.append(dict(zip(columns, row)))

        return result
    except pyodbc.Error as e:
        print(f"Database error occurred while getting form data: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        raise


def fetch_case_metadata(connection_string, os2formwebform_id):
    """Retrieve metadata for a specific os2formWebformId."""
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT os2formWebformId, tableName, caseType, hubUpdateResponseData,
                hubUpdateProcessStatus, caseData, documentData
                FROM [RPA].[rpa].Journalisering_Metadata
                WHERE os2formWebformId = ?
                """,
                (os2formwebform_id,)
            )
            row = cursor.fetchone()
            if row is not None:
                try:
                    case_data_parsed = json.loads(row.caseData) if row.caseData else None
                    document_data_parsed = json.loads(row.documentData) if row.documentData else None

                    case_data_parsed = {key: value.replace('\xa0', '') if isinstance(value, str) else value for key, value in case_data_parsed.items()}

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON data for form ID {os2formwebform_id}: {e}")
                    case_data_parsed = None
                    document_data_parsed = None

                case_metadata = {
                    'os2formWebformId': row.os2formWebformId,
                    'tableName': row.tableName,
                    'caseType': row.caseType,
                    'hubUpdateResponseData': row.hubUpdateResponseData,
                    'hubUpdateProcessStatus': row.hubUpdateProcessStatus,
                    'caseData': case_data_parsed,
                    'documentData': document_data_parsed
                }
                return case_metadata

            print(f"No data found for the given os2formWebformId: {os2formwebform_id}")
            return None
    except pyodbc.Error as e:
        print(f"Database error occurred: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error occurred while fetching metadata: {e}")
        raise


def _ensure_file_exists(file_path):
    """Checks if the file exists. If not, creates an empty file."""
    if not os.path.exists(file_path):
        raise OSError('File does not exists')
    else:
        print(f'File "{file_path}" already exists.')


def _ensure_folder_exists(full_path):
    """Checks if folder exists. If not, creates the folder."""
    folder_path = os.path.dirname(full_path)

    if not os.path.exists(folder_path):
        try:
            os.makedirs(folder_path)
            print(f'Folder "{folder_path}" has been created.')
        except OSError as e:
            print(f'Failed to create folder "{folder_path}". Reason: {e}')
            raise
    else:
        print(f'Folder "{folder_path}" already exists.')


def _delete_file(full_path: str):
    """Deletes the given file."""
    if os.path.isfile(full_path):
        try:
            os.remove(full_path)
            print(f'File "{full_path}" has been deleted.')
        except OSError as e:
            print(f'Failed to delete file "{full_path}". Reason: {e}')
            raise
    else:
        print(f'File "{full_path}" does not exist or is not a valid file.')


def download_receipt(url: str, api_key: str, full_path: str):
    """Downloads the receipt for the given form"""
    try:
        _ensure_folder_exists(full_path)
        _delete_file(full_path)
        file_bytes = download_file_bytes(url=url, os2_api_key=api_key)
        with open(full_path, 'wb') as file:
            file.write(file_bytes)
        print(f"File created: {full_path}")
    except OSError as e:
        print(f"File system error when attempting to download the receipt. {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during receipt download: {e}")
        raise


def get_credential(orchestrator_connection, credential_name):
    """Helper function to fetch credentials and handle potential errors."""
    try:
        return orchestrator_connection.get_credential(credential_name)
    except RuntimeError as e:
        print("Error fetching credential %s: %s", credential_name, e)
        raise


def get_constant(orchestrator_connection, constant_name):
    """Helper function to fetch constants and handle potential errors."""
    try:
        return orchestrator_connection.get_constant(constant_name).value
    except RuntimeError as e:
        print("Error fetching constant %s: %s", constant_name, e)
        raise


def process_orchestration_arguments(orchestrator_connection):
    """Helper function to process orchestration arguments."""
    try:
        return json.loads(orchestrator_connection.process_arguments)
    except RuntimeError as e:
        print("Error parsing process arguments: %s", e)
        raise


def get_case_metadata_and_forms(conn_db_rpa, webform_id, hub_table_name):
    """Helper function to fetch case metadata and forms."""
    try:
        case_metadata = fetch_case_metadata(connection_string=conn_db_rpa, os2formwebform_id=webform_id)
        forms = get_next_form(connection_string=conn_db_rpa, table_name=hub_table_name)
        return case_metadata, forms
    except RuntimeError as e:
        print("Error fetching case metadata or forms: %s", e)
        raise


def _get_note_message(form_consent, case_metadata, clinic_name, clinic_address):
    """Creates the journal note message based on consent status."""
    if form_consent == "1":
        note_template = case_metadata.get('caseData', {}).get('note', [{}])[0].get('noteMessageConsent', '')
    else:
        note_template = case_metadata.get('caseData', {}).get('note', [{}])[0].get('noteMessageNoConsent', '')

    return note_template.replace('[tandlæge]', clinic_name).replace('[Adresse]', clinic_address)


def initalize_solteq_tand(solteq_tand_creds):
    """MISSING"""
    app_obj = SolteqTandApp(
        app_path=config.APP_PATH,
        username=solteq_tand_creds.username,
        password=solteq_tand_creds.password,
    )
    app_obj.start_application()
    app_obj.login()

    return app_obj


def handle_form(app_obj, form, case_metadata, os2forms_api_key, conn_db_rpa, conn_db_solteq_tand, solteq_tand_creds, ssn):
    """Handles the processing of an individual form."""
    form_ssn = ssn
    full_path = os.path.join(config.PATH_TO_FILE, case_metadata.get('documentData', {}).get('fileName', None))

    try:
        app_obj.open_patient(ssn=form_ssn)

    except PatientOpenError as e:
        print(f"Exception caught: {e}")
        raise

    try:
        receipt_url = form.get('url')
        # download_receipt(url=receipt_url, api_key=os2forms_api_key.password, full_path=full_path)

        _ensure_file_exists(full_path)

        app_obj.create_document(
            document_full_path=full_path,
            document_type=case_metadata.get('documentData', {}).get('documentType', None)
        )

        db_obj = SolteqTandDatabase(
            conn_str=conn_db_solteq_tand,
            ssn=form_ssn
        )

        primary_dental_clinic = db_obj.get_primary_dental_clinic()
        primary_dental_clinic_name = primary_dental_clinic.get('data', {}).get('preferredDentalClinicName')

        app_obj.create_event(
            event_message=case_metadata.get('caseData', {}).get('event', {}).get('eventMessage', None),
            patient_clinic=primary_dental_clinic_name
        )

        forn_clinic_name = form.get('klinik_navn')
        forn_clinic_address = form.get('klinik_adresse')
        form_consent = form.get('samtykke_til_journaloverdragelse')
        note_message = _get_note_message(form_consent, case_metadata, forn_clinic_name, forn_clinic_address)

        app_obj.create_journal_note(note_message=note_message, checkmark_in_complete=True)

        table_name = case_metadata.get('tableName', None)
        stored_procedure = case_metadata.get('hubUpdateProcessStatus', None)
        uuid = form.get('uuid')
        status_params = {
            "Status": ("str", "Successful"),
            "uuid": ("str", f'{uuid}'),
            "TableName": ("str", f'{table_name}')
        }
        execute_stored_procedure(connection_string=conn_db_rpa, stored_procedure=stored_procedure, params=status_params)

        app_obj.close_patient_window()

    except (RuntimeError, OSError) as e:
        print(f"Exception caught: {e}")
        raise e

    finally:
        _delete_file(full_path)
