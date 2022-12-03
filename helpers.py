def literal_eval(string):
    try:
        return eval(string)
    except:
        return string
