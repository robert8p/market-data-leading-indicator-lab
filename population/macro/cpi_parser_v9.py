"""CPI retained-original extraction. Produces evidence, never acceptance.
Requires lxml. Exact source bytes and release context are mandatory.
"""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from zoneinfo import ZoneInfo
import re
from lxml import html

REVISION = "CTX07_CPI_TABLE_HEADERS_V9"
FACTORS = {
 "cpi_index_nsa_as_released": ("All items", "index"),
 "cpi_core_index_nsa_as_released": ("All items less food and energy", "index"),
 "cpi_mom_as_released": ("All items", "mom"),
 "cpi_core_mom_as_released": ("All items less food and energy", "mom"),
 "cpi_yoy_as_released": ("All items", "yoy"),
 "cpi_core_yoy_as_released": ("All items less food and energy", "yoy"),
}
def text(node):
    return " ".join(node.text_content().split())
def compact(value):
    return re.sub(r"[\s.]+", "", value).lower()
def one(items, reason):
    if len(items) != 1: raise ValueError(reason + ":" + str(len(items)))
    return items[0]
def table(doc, identifier):
    return one(doc.xpath("//table[@id=$id]", id=identifier), "TABLE_NOT_UNIQUE")
def label_month(dt):
    return dt.strftime("%b%Y").lower()
def month_before(dt):
    return dt.replace(year=dt.year-1,month=12) if dt.month==1 else dt.replace(month=dt.month-1)
def extract_cell(tab, row_label, group_label, period_label):
    row=one([r for r in tab.xpath(".//tbody/tr") if r.xpath("./th") and text(r.xpath("./th")[0])==row_label], "ROW_NOT_UNIQUE")
    row_id=row.xpath("./th")[0].get("id")
    group=one([h for h in tab.xpath(".//thead//th") if text(h)==group_label], "HEADER_GROUP_NOT_UNIQUE")
    period=one([h for h in tab.xpath(".//thead//th") if group.get("id") in h.get("headers","").split() and compact(text(h))==period_label], "PERIOD_HEADER_NOT_UNIQUE")
    expected={row_id,group.get("id"),period.get("id")}
    cell=one([d for d in row.xpath("./td") if set(d.get("headers","").split())==expected], "CELL_NOT_UNIQUE")
    lexeme=text(cell)
    if not re.fullmatch(r"-?\d+\.\d+",lexeme): raise ValueError("NON_DECIMAL_SOURCE_CELL")
    return {"value":str(Decimal(lexeme)),"numeric_lexeme":lexeme,"row_label":row_label,
            "header_group":group_label,"period_header":text(period),"table_id":tab.get("id"),
            "cell_headers":cell.get("headers"),"source_line":cell.sourceline}
def extract(body:bytes, expected_sha:str, factor:str, reference:str, release:str):
    if len(body)>8*1024*1024: raise ValueError("SOURCE_BYTE_LIMIT")
    if sha256(body).hexdigest()!=expected_sha: raise ValueError("SOURCE_HASH_MISMATCH")
    if factor not in FACTORS: raise ValueError("UNSUPPORTED_FACTOR")
    doc=html.fromstring(body)
    ref=datetime.fromisoformat(reference).astimezone(ZoneInfo("America/New_York"))
    rel=datetime.fromisoformat(release).astimezone(timezone.utc)
    pres=[p.text_content() for p in doc.xpath("//pre") if "Transmission of material in this release is embargoed until" in p.text_content()]
    pre=one(pres,"PUBLICATION_HEADER_NOT_UNIQUE")
    match=one(re.findall(r"8:30 a\.m\. \(ET\) (Monday|Tuesday|Wednesday|Thursday|Friday), ([A-Za-z]+ \d{1,2}, \d{4})",pre),"PUBLICATION_DATE_NOT_UNIQUE")
    published=datetime.strptime(match[1],"%B %d, %Y").replace(hour=8,minute=30,tzinfo=ZoneInfo("America/New_York"))
    if published.strftime("%A")!=match[0] or published.astimezone(timezone.utc)!=rel: raise ValueError("PUBLICATION_TIME_MISMATCH")
    if "CONSUMER PRICE INDEX - "+ref.strftime("%B %Y").upper() not in pre: raise ValueError("REFERENCE_PERIOD_MISMATCH")
    tab=table(doc,"cpipress1")
    caption=one(tab.xpath("./caption"),"CAPTION_NOT_UNIQUE")
    cap=text(caption)
    if "Consumer Price Index for All Urban Consumers (CPI-U): U.S. city average" not in cap or ref.strftime("%B %Y") not in cap or "1982-84=100" not in cap: raise ValueError("INDEX_CAPTION_MISMATCH")
    row,kind=FACTORS[factor]
    current=label_month(ref)
    if kind=="index": group,period="Unadjusted indexes",current
    elif kind=="mom": group,period="Seasonally adjusted percent change",label_month(month_before(ref))+"-"+current
    else: group,period="Unadjusted percent change",label_month(ref.replace(year=ref.year-1))+"-"+current
    out=extract_cell(tab,row,group,period)
    if kind!="index":
        a=table(doc,"cpi_pressa")
        ar=one([r for r in a.xpath(".//tbody/tr") if r.xpath("./th") and text(r.xpath("./th")[0])==row],"TABLE_A_ROW_NOT_UNIQUE")
        if kind=="mom":
            cross=extract_cell(a,row,"Seasonally adjusted changes from preceding month",current)
        else:
            h=one([h for h in a.xpath(".//thead//th") if compact(text(h))=="un-adjusted12-mosended"+current],"TABLE_A_YEAR_HEADER")
            rid=ar.xpath("./th")[0].get("id")
            cell=one([d for d in ar.xpath("./td") if set(d.get("headers","").split())=={rid,h.get("id")}],"TABLE_A_YEAR_CELL")
            cross={"value":str(Decimal(text(cell))),"numeric_lexeme":text(cell),"table_id":"cpi_pressa","cell_headers":cell.get("headers"),"source_line":cell.sourceline}
        if Decimal(cross["value"])!=Decimal(out["value"]): raise ValueError("CROSS_TABLE_NUMERICAL_MISMATCH")
        out["cross_table"]=cross
    out.update({"factor_name":factor,"units":"index points" if kind=="index" else "percent",
        "reference_period":reference,"published_at":published.astimezone(timezone.utc).isoformat(),
        "source_sha256":expected_sha,"parser_revision":REVISION,"availability_policy":"PUBLICATION_SUPPORTED_RECONSTRUCTION",
        "historical_received_at":None,"independent_review":False,"family_accepted":False})
    return out

