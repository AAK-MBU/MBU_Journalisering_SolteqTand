"""This module handles resetting the state of the computer so the robot can work with a clean slate."""

import os

import psutil
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from psutil import AccessDenied, NoSuchProcess, ZombieProcess


def reset(orchestrator_connection: OrchestratorConnection) -> None:
    """Clean up, close/kill all programs and start them again."""
    orchestrator_connection.log_trace("Resetting.")
    clean_up(orchestrator_connection)
    close_all(orchestrator_connection)
    kill_all(orchestrator_connection)
    open_all(orchestrator_connection)


def clean_up(orchestrator_connection: OrchestratorConnection) -> None:
    """Do any cleanup needed to leave a blank slate."""
    orchestrator_connection.log_trace("Doing cleanup.")


def close_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Gracefully close all applications used by the robot."""
    orchestrator_connection.log_trace("Closing all applications.")


def kill_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Forcefully close all applications used by the robot."""
    orchestrator_connection.log_trace("Killing all applications.")
    kill_process_by_name(orchestrator_connection, application_name="TMTand.exe")


# def kill_process_by_name(orchestrator_connection: OrchestratorConnection, process_name: str):
#     """Kills all processes with the specified name."""
#     orchestrator_connection.log_trace(f"Searching for process: {process_name}.")
#     for proc in psutil.process_iter(['pid', 'name']):
#         if proc.info['name'] == process_name:
#             orchestrator_connection.log_trace(f"Killing {proc.info['name']}.")
#             proc.kill()
#             orchestrator_connection.log_trace(f"Killed process {proc.info['name']} with PID {proc.info['pid']}")


def kill_process_by_name(
    orchestrator_connection: OrchestratorConnection, application_name: str
):
    """Kills all processes with the specified name."""
    target = application_name.lower()
    orchestrator_connection.log_trace(f"Killing {application_name} processes.")

    procs = []
    for proc in psutil.process_iter(
        attrs=["pid", "name", "exe", "cmdline"], ad_value=None
    ):
        try:
            name = (proc.info.get("name") or "").lower()
            exe_base = os.path.basename(proc.info.get("exe") or "").lower()
            if target in (name, exe_base):
                procs.append(proc)
        except (NoSuchProcess, ZombieProcess):
            continue
        except Exception as e:
            orchestrator_connection.log_trace(
                f"While enumerating {application_name}, skipping PID {getattr(proc, 'pid', '?')}: {e}"
            )

    # Try graceful terminate first
    for proc in procs:
        try:
            proc.terminate()
        except (NoSuchProcess, ZombieProcess):
            continue
        except AccessDenied as e:
            orchestrator_connection.log_trace(
                f"Access denied terminating {application_name} (PID {proc.pid}): {e}"
            )
        # pylint: disable-next = broad-exception-caught
        except Exception as e:
            orchestrator_connection.log_trace(
                f"Unexpected error terminating {application_name} (PID {proc.pid}): {e}"
            )

    # Wait a moment, then force kill stragglers
    gone, alive = psutil.wait_procs(procs, timeout=5)

    for p in gone:
        orchestrator_connection.log_trace(
            f"{application_name} (PID {p.pid}) exited cleanly."
        )

    for proc in alive:
        try:
            proc.kill()
        except (NoSuchProcess, ZombieProcess):
            continue
        except AccessDenied as e:
            orchestrator_connection.log_trace(
                f"Access denied killing {application_name} (PID {proc.pid}): {e}"
            )
        # pylint: disable-next = broad-exception-caught
        except Exception as e:
            orchestrator_connection.log_trace(
                f"Unexpected error killing {application_name} (PID {proc.pid}): {e}"
            )


def open_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Open all programs used by the robot."""
    orchestrator_connection.log_trace("Opening all applications.")
