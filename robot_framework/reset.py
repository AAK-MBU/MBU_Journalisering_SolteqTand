"""This module handles resetting the state of the computer so the robot can work with a clean slate."""

import psutil

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection


def reset(orchestrator_connection: OrchestratorConnection) -> None:
    """Clean up, close/kill all programs and start them again. """
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
    kill_process_by_name(orchestrator_connection, process_name="TMTand.exe")


def kill_process_by_name(orchestrator_connection: OrchestratorConnection, process_name: str):
    """Kills all processes with the specified name."""
    orchestrator_connection.log_trace(f"Searching for process: {process_name}.")
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'] == process_name:
            orchestrator_connection.log_trace(f"Killing {proc.info['name']}.")
            proc.kill()
            orchestrator_connection.log_trace(f"Killed process {proc.info['name']} with PID {proc.info['pid']}")


def open_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Open all programs used by the robot."""
    orchestrator_connection.log_trace("Opening all applications.")
