import re, json, time, urllib.request, urllib.error
from typing import List

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

PDF_RX = re.compile(r'href=["' + "'" + r']([^"' + "'" + r']+\.pdf(?:\?[^"' + "'" + r'"]*)?)["' + "'" + r']', re.I)

def fetch_pdf_links_from_rebrickable_html(set_num: str, timeout: float = 15.0) -> List[str]:
    url = f"https://rebrickable.com/sets/{set_num}/"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return []
            html = resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        # 403/404 are common; just return empty
        return []
    except Exception:
        return []
    # Grep PDFs from HTML (case-insensitive; de-dupe)
    pdfs = list(dict.fromkeys(PDF_RX.findall(html)))
    # Normalize relative links if any appear (rare)
    out = []
    for p in pdfs:
        if p.startswith("//"):
            out.append("https:" + p)
        elif p.startswith("/"):
            out.append("https://rebrickable.com" + p)
        else:
            out.append(p)
    return out
