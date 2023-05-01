def deduplicate(x: list) -> list:
    return list(dict.fromkeys(x))


def take_identifier(url):
    return url.rsplit('/', 1)[-1]
