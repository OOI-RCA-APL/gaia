import os
import shutil
import subprocess

from gaia.environment import WINDOWS


def start(
    root: str,
    app: str,
    port: int,
    reload: bool = False,
    workers: int = 1,
) -> None:
    """
    Start the application server in a from a root directory on a provided port given the module path
    of the ASGI app instance.

    :param root: The path of the root directory to run the app from.
    :param app: The module path of the ASGI app to run with Uvicorn. Example: app.main:app
    :param port: The port to serve the app on.
    :param reload: If true, the app will automatically restart when source code is changed.
    :param workers: The number of worker processes that should be started and managed by Circus.
    """
    # Circus doesn't run on Windows, so just limit the number of workers to 1.
    if WINDOWS:
        workers = 1

    # Store the original location.
    location = os.getcwd()
    os.chdir(root)

    poetry = shutil.which("poetry") or "/usr/local/bin/poetry"

    command = [
        f"{poetry} run uvicorn {app}",
        "--host 0.0.0.0",
        "--lifespan on",
    ]

    # If "reload" is specified, enable Uvicorn's "--reload" option.
    if reload:
        command.append("--reload")

    if workers > 1:
        # If we're running more than one worker process, use Circus to manage them.
        from circus import get_arbiter
        from circus.sockets import CircusSocket

        # Bind Uvicorn to the file descriptor of the socket we declare in the "sockets" section.
        command.append("--fd $(circus.sockets.web)")

        arbiter = get_arbiter(
            [
                {
                    "cmd": " ".join(command),  # Specify the command for each process.
                    "numprocesses": workers,  # Specify the number of processes.
                    "copy_env": True,  # Copy environment variables into the sub-process.
                    "use_sockets": True,  # Allow socket usage.
                    "stdout_stream": {"class": "circus.stream.StdoutStream"},  # Print stdout.
                    "stderr_stream": {"class": "circus.stream.StdoutStream"},  # Print stderr.
                },
            ],
            sockets=[
                # Declare a socket named "web" that all worker processes will bind to.
                CircusSocket(
                    name="web",
                    host="0.0.0.0",
                    port=port,
                    replace=True,
                )
            ],
        )

        try:
            # Start worker processes.
            arbiter.start()
        finally:
            # Handle exit.
            arbiter.stop()
    else:
        # Otherwise, run the subprocess directly.
        command.append(f"--port {port}")
        subprocess.call(" ".join(command), shell=True)

    # Go back to the original location.
    os.chdir(location)
