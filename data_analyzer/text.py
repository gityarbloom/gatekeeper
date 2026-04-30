import html
import re



class TextPreparer:       

    @staticmethod
    def clean_html(row_text:str):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', row_text)
        clean_text = html.unescape(cleantext)
        return clean_text

    @staticmethod
    def price_extraction(text:str):
        match = re.search(r'\$(\d+)', text)
        if match:
            return int(match.group(1))
        return 0