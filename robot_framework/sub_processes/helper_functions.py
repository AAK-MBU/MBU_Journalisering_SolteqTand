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
                    JSON_VALUE(form_data, '$.data.adresse') AS klinik_adresse,
                    JSON_VALUE(form_data, '$.data.tandlaege') AS klinik_navn,
                    (
                        SELECT TOP 1 JSON_VALUE(a.value, '$.url')
                        FROM OPENJSON(JSON_QUERY(form_data, '$.data.attachments')) a
                    ) AS url,
                    form_data
            FROM    [RPA].[journalizing].[view_Journalizing]
            WHERE   status = 'New'
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


def get_journal_note_data(form, case_metadata, consent_field):
    """
    Retrieves the appropriate journal note based on the consent_field value.

    :param form: Dictionary containing form data.
    :param case_metadata: Dictionary containing case metadata.
    :param consent_field: The key for the consent field to check.
    :return: A tuple containing the message and close_note values.
    """
    try:
        consent_field_value = None
        message = None
        close_note = None

        if consent_field:
            consent_field_value = get_node_value(form.get('form_data', {}), consent_field)

        if consent_field_value is None or consent_field_value == "1":
            message = case_metadata.get('caseData', {}).get('note', {}).get('noteMessage', {}).get('message', None)
            close_note = case_metadata.get('caseData', {}).get('note', {}).get('noteMessage', {}).get('closeNote', None)
        elif consent_field_value != "1":
            message = case_metadata.get('caseData', {}).get('note', {}).get('noteMessageNoConsent', {}).get('message', None)
            close_note = case_metadata.get('caseData', {}).get('note', {}).get('noteMessageNoConsent', {}).get('closeNote', None)

        return message, close_note

    except (Exception, RuntimeError) as e:
        print(f"Exception caught: {e}")
        raise e


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


def get_node_value(json_string, node_name):
    """
    Searches for a specific node in a JSON string and returns its value if it exists.

    :param json_string: The JSON string to search within.
    :param node_name: The name of the node to find.
    :return: The value of the node if it exists, or None if it doesn't.
    """
    try:
        # Parse the JSON string into a Python dictionary
        data = json.loads(json_string)

        # Recursively search for the node
        def search_node(data, target):
            if isinstance(data, dict):
                for key, value in data.items():
                    if key == target:
                        return value
                    result = search_node(value, target)
                    if result is not None:
                        return result
            elif isinstance(data, list):
                for item in data:
                    result = search_node(item, target)
                    if result is not None:
                        return result
            return None

        # Call the recursive function
        return search_node(data, node_name)
    except json.JSONDecodeError as e:
        print("Invalid JSON:", e)
        return None


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
        form_id = form.get('form_id', None)
        document_type = case_metadata.get('documentData', {}).get('documentType', None)

        filename = case_metadata.get('documentData', {}).get('fileName', None)
        full_path = os.path.join(config.PATH_TO_FILE, filename)

        receipt_url = form.get('url')
        download_receipt(url=receipt_url, api_key=os2forms_api_key.password, full_path=full_path)

        db_obj = SolteqTandDatabase(
            conn_str=conn_db_solteq_tand,
            ssn=ssn
        )

        # Check if document exists, if not then create the document in the file cabinet.
        document_exists = db_obj.check_if_document_exists(filename=filename, documenttype=document_type, form_id=form_id)
        if not document_exists:
            app_obj.create_document(
                document_full_path=full_path,
                document_type=document_type,
                document_description=form_id
            )
            sql_data_params = {
                "StepName": ("str", "Document"),
                "JsonFragment": ("str", json.dumps({"DocumentCreated": True})),
                "form_id": ("str", form_id)
            }
            execute_stored_procedure(connection_string=conn_db_rpa, stored_procedure=case_metadata.get('spUpdateResponseData', None), params=sql_data_params)

        # Check if event exists, if not then create the event.
        primary_dental_clinic = db_obj.get_primary_dental_clinic()
        primary_dental_clinic_name = primary_dental_clinic.get('data', {}).get('preferredDentalClinicName')

        event_message = case_metadata.get('caseData', {}).get('event', {}).get('message', None)
        is_archived = case_metadata.get('caseData', {}).get('event', {}).get('isArchived', None)

        event_exists = db_obj.check_if_event_exists(event_message=event_message, event_name=primary_dental_clinic_name, is_archived=is_archived)
        if not event_exists:
            app_obj.create_event(
                event_message=event_message,
                patient_clinic=primary_dental_clinic_name
            )
            sql_data_params = {
                "StepName": ("str", "Event"),
                "JsonFragment": ("str", json.dumps({"EventCreated": True})),
                "form_id": ("str", form_id)
            }
            execute_stored_procedure(connection_string=conn_db_rpa, stored_procedure=case_metadata.get('spUpdateResponseData', None), params=sql_data_params)

        # Check if journal note exists, if not then create the note.
        clinic_name = form.get('klinik_navn') if form.get('klinik_navn') is not None else "[Ingen]"
        clinic_address = form.get('klinik_adresse') if form.get('klinik_adresse') is not None else "[Ingen]"

        consent_field = case_metadata.get('caseData', {}).get('note', {}).get('consentField', None)
        note_message, close_note = get_journal_note_data(form, case_metadata, consent_field)

        message_modified = note_message.replace('[tandlæge]', clinic_name).replace('[Adresse]', clinic_address)
        substrings_to_remove = ["Administrativt notat ", "'"]
        cleaned_note_message = _clean_note_message(message_modified, substrings_to_remove)

        journal_note_exists = db_obj.get_journal_notes(note_message=cleaned_note_message)
        if not journal_note_exists:
            app_obj.create_journal_note(note_message=message_modified, checkmark_in_complete=close_note)
            sql_data_params = {
                "StepName": ("str", "JournalNote"),
                "JsonFragment": ("str", json.dumps({"JournalNoteCreated": True})),
                "form_id": ("str", form_id)
            }
            execute_stored_procedure(connection_string=conn_db_rpa, stored_procedure=case_metadata.get('spUpdateResponseData', None), params=sql_data_params)

        # Update form status in the database.
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
