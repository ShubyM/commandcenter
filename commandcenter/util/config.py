import pathlib



def cast_path(path: str | None) -> pathlib.Path:
    """Cast a non-empty string to a path."""
    if path:
        return pathlib.Path(path)
    return path


def cast_logging_level(level: str | int) -> int:
    """Cast a logging level as str or int to int."""
    try:
        return int(level)
    except ValueError:
        match level.lower():
            case "notset":
                return 0
            case "debug":
                return 10
            case "info":
                return 20
            case "warning":
                return 30
            case "error":
                return 40
            case "critical":
                return 50
            case _:
                return 0