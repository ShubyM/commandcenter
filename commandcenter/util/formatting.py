import re



pattern = re.compile('(?<!^)(?=[A-Z])')


def snake_to_camel(string: str) -> str:
    """Covert snake case (arg_a) to camel case (ArgA)."""
    return ''.join(word.capitalize() for word in string.split('_'))


def snake_to_lower_camel(string: str) -> str:
    """Covert snake case (arg_a) to lower camel case (argA)."""
    splits = string.split('_')
    if len(splits) == 1:
        return string
    return f"{splits[0]}{''.join(word.capitalize() for word in splits[1:])}"


def camel_to_snake(string: str) -> str:
    """Convert camel (ArgA) and lower camel (argB) to snake case (arg_a)"""
    return pattern.sub('_', string).lower()