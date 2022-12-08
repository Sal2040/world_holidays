def literal_eval(string):
    try:
        return eval(string)
    except:
        return string


def dict_to_list(dictionary):
    if not isinstance(dictionary, dict):
        return dictionary
    else:
        return list(dictionary.values())
