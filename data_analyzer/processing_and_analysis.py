import html
import re



def clean_html(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return html.unescape(cleantext)


def extraction_the_price(text):
    match = re.search(r'\$(\d+)', text)
    if match:
        return match.group(1)




# tx = "The <b>package</b> will be delivered at <i>midnight</i> near point X. Use the rifle. Payment: $15000 on delivery."
# tx2 = clean_html(tx)
# print(extraction_the_price(tx2))