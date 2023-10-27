from incline import __version__
import platform
import importlib.metadata
from opentelemetry.util._importlib_metadata import version


def incline_version() -> dict[str, str]:
    return {
        "incline": __version__,
        "python": platform.python_version(),
        "boto3": importlib.metadata.version("boto3"),
        "opentelemetry-api": version("opentelemetry-api"),
        "opentelemetry-sdk": version("opentelemetry-sdk")
    }
