import random


def generate_temporary_path_name(path, num=random.randrange(0, 10000000000)):
    # type: (str, int) -> str
    """
    Returns a temporary path.

    :param path: Original path.
    :param num: random number

    :return: Temporary path.
    """
    tmp_name = "{}-temp-{:010d}".format(path, num)  # type: str
    return tmp_name
