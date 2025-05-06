import tempfile


def _create_temp_file(content: str | bytes, suffix: str) -> str:
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temp_file:
        temp_file.write(content.encode())
        return temp_file.name


def str_to_file(content: str, suffix: str) -> str | None:
    """
    If content is not None, create a temporary file with the given content and suffix.

    :param content: The content to write to the file.
    :param suffix: The suffix for the temporary file.
    :return: The path to the temporary file or None if no content is provided.
    """
    path = None
    if content:
        path = _create_temp_file(content, suffix)
    return path
