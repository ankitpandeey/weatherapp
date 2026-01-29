import unicodedata
import re

def clean_text(text):
    if text is None:
        return None

    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-zA-Z0-9\s\-]", "", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text