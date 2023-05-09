def to_bool(value):
    if value == True:
        return value
    value = str(value)
    if value == "True" or value == "TRUE" or value == "true":
        return True
    else:
        return False
