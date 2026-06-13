def default_serializer(_input):
    if isinstance(_input, str):
        return _input.encode("utf-8")
    return _input