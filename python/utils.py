

def format_field_name(text: str):
    return "_".join(text.strip().replace(":", "").replace(".", "").lower().split(" "))
