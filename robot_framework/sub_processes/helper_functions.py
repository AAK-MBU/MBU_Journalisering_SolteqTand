"""
This module provides a set of helper functions and classes for interacting with a SQL database, processing forms,
handling files, and integrating with the Solteq Tand application. It includes functionality to retrieve form data,
metadata, manage files, and execute application-specific processes such as uploading receipts, creating journal notes,
and handling patient records ect.
"""
import time
import os
import json
import pyodbc
from mbu_dev_shared_components.os2forms.documents import download_file_bytes
from mbu_dev_shared_components.solteqtand.app_handler import SolteqTandApp, ManualProcessingRequiredError
from mbu_dev_shared_components.solteqtand.db_handler import SolteqTandDatabase
from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure
from robot_framework import config


def get_forms(connection_string, form_type):
    """Fetches the next available form from a specified table in the SQL database.

    Args:
        connection_string (str): The connection string to the database.
        table_name (str): The name of the table from which to fetch form data.

    Returns:
        list[dict]: A list of dictionaries where each dictionary contains data related to a form.

    Raises:
        pyodbc.Error: If an error occurs while accessing the database.
        Exception: For any other unexpected errors.
    """
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT  form_id,
                    JSON_VALUE(form_data, '$.data.cpr_nummer_barn') AS cpr_barn,
                    JSON_VALUE(form_data, '$.data.cpr_nummer') AS cpr_voksen,
                    JSON_VALUE(form_data, '$.data.jeg_giver_tilladelse_til_at_tandplejen_aarhus_maa_sende_journal_') AS samtykke_til_journaloverdragelse,
                    JSON_VALUE(form_data, '$.data.adresse') AS klinik_adresse,
                    JSON_VALUE(form_data, '$.data.tandlaege') AS klinik_navn,
					(
                        SELECT TOP 1 JSON_VALUE(a.value, '$.url') 
						FROM OPENJSON(JSON_QUERY(form_data, '$.data.attachments')) a
					) AS url
            FROM    [RPA].[journalizing].[view_Journalizing]
            WHERE   status IS NULL
                    AND form_type = ?
            """,
            (form_type,)
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
                SELECT os2formWebformId, caseType, spUpdateResponseData,
                spUpdateProcessStatus, caseData, documentData
                FROM [RPA].[journalizing].[Metadata]
                WHERE os2formWebformId = ?""",
                (os2formwebform_id,)
            )
            row = cursor.fetchone()
            if row is not None:

                try:
                    case_data_parsed = json.loads(row.caseData) if row.caseData else None
                    document_data_parsed = json.loads(row.documentData) if row.documentData else None

                    # Clean up the case data by removing non-breaking spaces
                    case_data_parsed = {key: value.replace('\xa0', '') if isinstance(value, str) else value for key, value in case_data_parsed.items()}

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON data: {e}")
                    case_data_parsed = None
                    document_data_parsed = None

                case_metadata = {
                    'os2formWebformId': row.os2formWebformId,
                    'caseType': row.caseType,
                    'spUpdateResponseData': row.spUpdateResponseData,
                    'spUpdateProcessStatus': row.spUpdateProcessStatus,
                    'caseData': case_data_parsed,
                    'documentData': document_data_parsed
                }
                return case_metadata

            print("No data found for the given os2formWebformId.")
            return None

    except pyodbc.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def _ensure_file_exists(file_path):
    """Checks if the specified file exists.

    Args:
        file_path (str): The full path of the file to check.

    Raises:
        OSError: If the file does not exist.
    """
    if not os.path.exists(file_path):
        raise OSError('File does not exists')

    print(f'File "{file_path}" exists.')


def _ensure_folder_exists(full_path):
    """Ensures that the folder for the given path exists. Creates the folder if it doesn't exist.

    Args:
        full_path (str): The full path where the folder should be created.

    Raises:
        OSError: If there is an error creating the folder.
    """
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
    """Deletes the specified file if it exists.

    Args:
        full_path (str): The full path of the file to be deleted.

    Raises:
        OSError: If an error occurs while trying to delete the file.
    """
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
    """Downloads the receipt from the given URL and saves it to the specified path.

    Args:
        url (str): The URL from which to download the receipt.
        api_key (str): The API key for authentication to access the URL.
        full_path (str): The full path where the receipt should be saved.

    Raises:
        OSError: If there is an error related to the file system.
        Exception: For any other unexpected errors during the download process.
    """
    try:
        _ensure_folder_exists(full_path)
        _delete_file(full_path)
        file_bytes = download_file_bytes(url=url, os2_api_key=api_key)
        with open(full_path, 'wb') as file:
            file.write(file_bytes)
        print(f"File created: {full_path}")
        _ensure_file_exists(full_path)
    except OSError as e:
        print(f"File system error when attempting to download the receipt. {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during receipt download: {e}")
        raise


def get_credential(orchestrator_connection, credential_name):
    """Fetches credentials from the orchestrator.

    Args:
        orchestrator_connection: The connection to the orchestrator.
        credential_name (str): The name of the credential to retrieve.

    Returns:
        The credential value.

    Raises:
        RuntimeError: If there is an error fetching the credential.
    """
    try:
        return orchestrator_connection.get_credential(credential_name)
    except RuntimeError as e:
        print("Error fetching credential: ", credential_name, e)
        raise


def get_constant(orchestrator_connection, constant_name):
    """Fetches a constant from the orchestrator.

    Args:
        orchestrator_connection: The connection to the orchestrator.
        constant_name (str): The name of the constant to retrieve.

    Returns:
        The constant value.

    Raises:
        RuntimeError: If there is an error fetching the constant.
    """
    try:
        return orchestrator_connection.get_constant(constant_name).value
    except RuntimeError as e:
        print("Error fetching constant: ", constant_name, e)
        raise


def process_orchestration_arguments(orchestrator_connection):
    """Processes the orchestration arguments by parsing them from JSON.

    Args:
        orchestrator_connection: The connection to the orchestrator.

    Returns:
        dict: The parsed arguments as a dictionary.

    Raises:
        RuntimeError: If there is an error parsing the arguments.
    """
    try:
        return json.loads(orchestrator_connection.process_arguments)
    except RuntimeError as e:
        print("Error parsing process arguments: ", e)
        raise


def get_journalize_metadata(conn_db_rpa, webform_id):
    """Fetches metadata for a given webform ID.

    Args:
        conn_db_rpa (str): The database connection string for RPA.
        webform_id (str): The ID of the webform.

    Returns:
        dict: A dictionary containing metadata.

    Raises:
        RuntimeError: If an error occurs while fetching metadata.
    """
    try:
        case_metadata = fetch_case_metadata(connection_string=conn_db_rpa, os2formwebform_id=webform_id)
        return case_metadata
    except RuntimeError as e:
        print("Error fetching metadata: ", e)
        raise


def _get_note_message(form_consent, case_metadata, clinic_name, clinic_address):
    """Generates a journal note message based on the consent status of the form.

    Args:
        form_consent (str): The consent status from the form (1 for consent, 0 for no consent).
        case_metadata (dict): The case metadata including templates for the note message.
        clinic_name (str): The name of the clinic.
        clinic_address (str): The address of the clinic.

    Returns:
        str: The formatted journal note message.
    """
    if form_consent == "1":
        note_template = case_metadata.get('caseData', {}).get('note', [{}])[0].get('noteMessageConsent', '')
    else:
        note_template = case_metadata.get('caseData', {}).get('note', [{}])[0].get('noteMessageNoConsent', '')

    return note_template.replace('[tandlæge]', clinic_name).replace('[Adresse]', clinic_address)


def initalize_solteq_tand(solteq_tand_creds):
    """Initializes the Solteq Tand application and logs in using the provided credentials.

    Args:
        solteq_tand_creds: Credentials object with username and password for the Solteq Tand app.

    Returns:
        SolteqTandApp: The initialized Solteq Tand application object.
    """
    app_obj = SolteqTandApp(
        app_path=config.APP_PATH,
        username=solteq_tand_creds.username,
        password=solteq_tand_creds.password,
    )
    app_obj.start_application()
    time.sleep(2)
    app_obj.login()

    return app_obj


def handle_form(app_obj, form, case_metadata, os2forms_api_key, conn_db_rpa, conn_db_solteq_tand, ssn):
    """Handles the processing of an individual form by interacting with Solteq Tand and updating the database.

    Args:
        app_obj (SolteqTandApp): The initialized Solteq Tand application object.
        form (dict): The form data to be processed.
        case_metadata (dict): The case metadata related to the form.
        os2forms_api_key (str): The API key for OS2Forms.
        conn_db_rpa (str): The database connection string for RPA.
        conn_db_solteq_tand (str): The database connection string for Solteq Tand.
        ssn (str): The social security number for the patient.

    Raises:
        ManualProcessingRequiredError: If further manual processing is needed.
        Exception: If any unexpected error occurs during the form handling process.
    """
    try:
        app_obj.open_patient(ssn=ssn)

    except ManualProcessingRequiredError as e:
        print(f"Exception caught: {e}")
        raise

    try:
        document_type = case_metadata.get('documentData', {}).get('documentType', None)
        filename = case_metadata.get('documentData', {}).get('fileName', None)
        form_id = form.get('form_id', None)
        full_path = os.path.join(config.PATH_TO_FILE, filename)

        receipt_url = form.get('url')
        download_receipt(url=receipt_url, api_key=os2forms_api_key.password, full_path=full_path)

        db_obj = SolteqTandDatabase(
            conn_str=conn_db_solteq_tand,
            ssn=ssn
        )

        document_exists = db_obj.check_if_document_exists(filename=filename, documenttype=document_type)
        if not document_exists:
            app_obj.create_document(
                document_full_path=full_path,
                document_type=document_type,
                document_description=form_id
            )

        primary_dental_clinic = db_obj.get_primary_dental_clinic()
        primary_dental_clinic_name = primary_dental_clinic.get('data', {}).get('preferredDentalClinicName')

        event_exists = db_obj.check_if_event_exists(event_message=case_metadata.get('caseData', {}).get('event', {}).get('eventMessage', None), event_name=primary_dental_clinic_name)
        if not event_exists:
            app_obj.create_event(
                event_message=case_metadata.get('caseData', {}).get('event', {}).get('eventMessage', None),
                patient_clinic=primary_dental_clinic_name
            )

        forn_clinic_name = form.get('klinik_navn') if form.get('klinik_navn') is not None else "[Ingen]"
        forn_clinic_address = form.get('klinik_adresse') if form.get('klinik_adresse') is not None else "[Ingen]"
        form_consent = form.get('samtykke_til_journaloverdragelse') if form.get('samtykke_til_journaloverdragelse') is not None else None

        note_message = _get_note_message(form_consent, case_metadata, forn_clinic_name, forn_clinic_address)

        def _clean_note_message(text, substrings):
            """
            Removes specified substrings from the input text.

            Args:
                text (str): The original string to be cleaned.
                substrings (list): A list of substrings to remove from the text.

            Returns:
                str: The cleaned string with specified substrings removed.
            """
            for substring in substrings:
                text = text.replace(substring, "")
            return text

        substrings_to_remove = ["Administrativt notat ", "'"]
        cleaned_note_message = _clean_note_message(note_message, substrings_to_remove)

        journal_note_exists = db_obj.get_journal_notes(note_message=cleaned_note_message)
        if not journal_note_exists:
            app_obj.create_journal_note(note_message=note_message, checkmark_in_complete=True)

        stored_procedure = case_metadata.get('spUpdateProcessStatus', None)
        form_id = form.get('form_id', None)
        status_params = {
            "Status": ("str", "Successful"),
            "form_id": ("str", f'{form_id}'),
        }
        execute_stored_procedure(connection_string=conn_db_rpa, stored_procedure=stored_procedure, params=status_params)

        app_obj.close_patient_window()

    except (Exception, RuntimeError, OSError) as e:
        print(f"Exception caught: {e}")
        raise e

    finally:
        _delete_file(full_path)
