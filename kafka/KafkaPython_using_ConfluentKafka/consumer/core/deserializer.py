def default_deserializer(_input):
    if _input:
        return _input.decode("utf-8")
    return None