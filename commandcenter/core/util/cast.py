import pathlib
from typing import Optional, Union

from commandcenter.core.util.enums import LogLevels



def cast_path(path: Optional[str]) -> pathlib.Path:
    """Cast a non-empty string to a path."""
    if path:
        return pathlib.Path(path)
    return path


def cast_logging_level(level: Union[str, int]) -> int:
    """Cast a logging level as str or int to int."""
    try:
        level = int(level)
    except ValueError:
        return LogLevels.get_intrep(level)
    if level not in [10, 20, 30, 40, 50]:
        return LogLevels.NOTSET.intrep
    return level