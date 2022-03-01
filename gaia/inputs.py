from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def get_input(parser: Callable[[str], T], prompt: str, default: Optional[T]) -> T:
    """
    Get input of a given type from the user. The first argument should be a function to parse the
    input text. If the parser throws an exception while parsing the input, the input will be
    requested again.

    :param parser: The function/class called to parse the input.
    :param prompt: The prompt to display to the user.
    :param default: The default value to return if the user enters an empty input.
    :return: The parsed input or default value.
    """
    while True:
        if default:
            text = input(f"{prompt} ({default}): ")
        else:
            text = input(f"{prompt}: ")

        if text == "":
            if default is not None:
                return default

            if isinstance(parser, type):
                if issubclass(parser, (bool, int, float)):
                    continue

        try:
            return parser(text)
        except:
            pass


def get_yes_no_input(prompt: str, default: Optional[bool] = None) -> bool:
    """
    Get a yes/no boolean input from the user with an optional default value.

    :param prompt: The prompt to display to the user.
    :param default: The default value to return if the user enters an empty input.
    :return: The input boolean or default value.
    """
    while True:
        if default is None:
            default_indicator = "y/n"
        elif default:
            default_indicator = "Y/n"
        else:
            default_indicator = "y/N"

        text = input(f"{prompt} ({default_indicator}): ").lower()
        if default is not None and text == "":
            return default
        if text in ("yes", "y"):
            return True
        if text in ("no", "n"):
            return False
