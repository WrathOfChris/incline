# type: ignore
from incline import __version__
import platform
import importlib.metadata


def incline_version() -> dict:
    return {
        "incline": __version__,
        "python": platform.python_version(),
        "boto3": importlib.metadata.version("boto3")
    }
