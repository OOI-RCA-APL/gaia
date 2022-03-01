import multiprocessing
import os
import platform


def _is_docker() -> bool:
    """
    Return `True` if the current process is running in a Docker container.
    """

    # Check for a ".dockerenv" file in the root directory.
    if os.path.exists("/.dockerenv"):
        return True

    # Check to see if the cgroup file exists and has a line containing the string "docker".
    group = "/proc/self/cgroup"
    if os.path.isfile(group):
        if any("docker" in line for line in open(group)):
            return True

    # If none of the above conditions are matched, assume we're not in a Docker container.
    return False


DOCKER = _is_docker()
"""
This will be true if the current process is running in a Docker container.
"""

LINUX = platform.system() == "Linux"
"""
This will be true if the current process is running on Linux.
"""

DARWIN = platform.system() == "Darwin"
"""
This will be true if the current process is running on MacOS/Darwin.
"""

WINDOWS = platform.system() == "Windows"
"""
This will be true if the current process is running on Windows.
"""

CPU_COUNT = multiprocessing.cpu_count()
"""
The number of CPUs available on the current device.
"""
