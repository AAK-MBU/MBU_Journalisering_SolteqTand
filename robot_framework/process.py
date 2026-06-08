"""This module contains the main process of the robot."""

from mbu_dev_shared_components.solteqtand.app_handler import (
    ManualProcessingRequiredError,
)
from mbu_dev_shared_components.utils.db_stored_procedure_executor import (
    execute_stored_procedure,
)
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from robot_framework import reset
from robot_framework.sub_processes import helper_functions as hf


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    oc_arg_json = hf.process_orchestration_arguments(
        orchestrator_connection=orchestrator_connection
    )

    solteq_tand_creds = hf.get_credential(
        orchestrator_connection=orchestrator_connection,
        credential_name="solteq_tand_svcrpambu001",
    )
    os2forms_api_key = hf.get_credential(
        orchestrator_connection=orchestrator_connection, credential_name="os2_api"
    )

    conn_db_rpa = hf.get_constant(
        orchestrator_connection=orchestrator_connection,
        constant_name="DbConnectionString",
    )
    conn_db_solteq_tand = hf.get_constant(
        orchestrator_connection=orchestrator_connection,
        constant_name="solteq_tand_db_connstr",
    )

    webform_id = oc_arg_json.get("webformId")

    case_metadata = hf.get_journalize_metadata(
        conn_db_rpa=conn_db_rpa, webform_id=webform_id
    )

    forms = hf.get_forms(connection_string=conn_db_rpa, form_type=webform_id)

    stored_procedure = case_metadata.get("spUpdateProcessStatus", None)

    if forms:
        app_obj = hf.initalize_solteq_tand(solteq_tand_creds=solteq_tand_creds)

        for form in forms:
            try:
                if form.get("cpr_barn") is not None:
                    form_ssn = form.get("cpr_barn")
                else:
                    form_ssn = form.get("cpr_voksen")

                if form_ssn is None or form_ssn == "":
                    orchestrator_connection.log_error("No SSN found!.")
                    raise ManualProcessingRequiredError

                form_ssn_not_in_list = form.get("mit_barn_kommer_ikke_frem_i_listen")
                if form_ssn_not_in_list == "1":
                    orchestrator_connection.log_error("SSN not in list is marked.")
                    raise ManualProcessingRequiredError

                hf.handle_form(
                    app_obj,
                    form,
                    case_metadata,
                    os2forms_api_key,
                    conn_db_rpa,
                    conn_db_solteq_tand,
                    form_ssn,
                )

            except ManualProcessingRequiredError:
                orchestrator_connection.log_error(
                    "Manual processing is needed. The form is added to a manuel list."
                )
                update_db_form_status(
                    connection_string=conn_db_rpa,
                    status="Manuel",
                    form_id=form.get("form_id"),
                    stored_procedure=stored_procedure,
                )

            except Exception:
                orchestrator_connection.log_error("Error occurred.")
                update_db_form_status(
                    connection_string=conn_db_rpa,
                    status="Failed",
                    form_id=form.get("form_id"),
                    stored_procedure=stored_procedure,
                )
                _close_solteq_tand_safe(app_obj, orchestrator_connection)
                raise

        _close_solteq_tand_safe(app_obj, orchestrator_connection)


def _close_solteq_tand_safe(
    app_obj, orchestrator_connection: OrchestratorConnection
) -> None:
    """Attempt a graceful close of Solteq Tand.
    If close_solteq_tand() times out or raises (e.g. the expected 'Ja' dialog
    is not present because the app is in an unexpected state), fall back to
    force-killing the TMTand.exe process via reset.kill_process_by_name so
    the robot always leaves a clean slate regardless of UI state.
    """
    try:
        app_obj.close_solteq_tand()
    except Exception as close_err:  # pylint: disable=broad-exception-caught
        orchestrator_connection.log_error(
            f"Graceful close of Solteq Tand failed: {close_err}. "
            "Falling back to force-kill of TMTand.exe."
        )
        reset.kill_process_by_name(
            orchestrator_connection, application_name="TMTand.exe"
        )


def update_db_form_status(connection_string, status, form_id, stored_procedure):
    """Updates status."""
    status_params = {"Status": ("str", f"{status}"), "form_id": ("str", f"{form_id}")}
    execute_stored_procedure(
        connection_string=connection_string,
        stored_procedure=stored_procedure,
        params=status_params,
    )
