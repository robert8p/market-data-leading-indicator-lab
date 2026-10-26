"""Tests use retained private files; no original bodies belong in Git."""
import copy,json,pathlib,sys
from hashlib import sha256
from decimal import Decimal
from lxml import html
from cpi_parser_v9 import extract
p=pathlib.Path(sys.argv[1] if len(sys.argv)>1 else ".")
observations=json.loads((p/"observations.json").read_text())
results=[]
for o in observations:
 b=(p/(o["source_id"]+".html")).read_bytes()
 e=extract(b,o["source_content_sha256"],o["factor_name"],o["reference_period"],o["released_at"])
 assert Decimal(e["value"])==Decimal(o["actual"]) and e["units"]==o["units"]
 assert e["historical_received_at"] is None and e["family_accepted"] is False
 results.append({"case":"retained:"+o["observation_id"],"pass":True})
o=next(x for x in observations if x["factor_name"]=="cpi_mom_as_released" and x["source_id"]=="cpi_202607")
body=(p/(o["source_id"]+".html")).read_bytes()
def rejects(name,b=body,hash_override=None,factor=None,reference=None,release=None):
 try:
  extract(b,hash_override or sha256(b).hexdigest(),factor or o["factor_name"],reference or o["reference_period"],release or o["released_at"])
 except (ValueError,KeyError):
  results.append({"case":name,"pass":True})
 else:raise AssertionError(name)
rejects("hash_tamper",body+b" ",hash_override=o["source_content_sha256"])
rejects("unsupported_factor",factor="invented")
rejects("future_release",release="2026-09-12T12:30:00+00:00")
rejects("wrong_reference",reference="2026-08-01T04:00:00+00:00")
def mutated(name,change):
 d=html.fromstring(body);change(d);rejects(name,html.tostring(d,encoding="utf-8"))
mutated("duplicate_table",lambda d:d.append(copy.deepcopy(d.get_element_by_id("cpipress1"))))
mutated("duplicate_row",lambda d:d.get_element_by_id("cpipress1.r.1").getparent().getparent().append(copy.deepcopy(d.get_element_by_id("cpipress1.r.1").getparent())))
mutated("wrong_group",lambda d:setattr(d.get_element_by_id("cpipress1.h.1.8"),"text","Unadjusted percent change"))
mutated("wrong_period",lambda d:d.get_element_by_id("cpipress1.h.2.10").clear())
mutated("non_numeric_cell",lambda d:setattr(d.xpath('//td[@headers="cpipress1.r.1 cpipress1.h.1.8 cpipress1.h.2.10"]/span')[0],"text","not available"))
mutated("cross_table_mismatch",lambda d:setattr(d.xpath('//td[@headers="cpi_pressa.r.1 cpi_pressa.h.1.2 cpi_pressa.h.2.8"]/span')[0],"text","9.9"))
mutated("wrong_index_basis",lambda d:d.get_element_by_id("cpipress1").xpath("./caption")[0].clear())
mutated("wrong_weekday",lambda d:setattr(d.xpath("//pre")[0],"text",d.xpath("//pre")[0].text.replace("Wednesday, August 12","Thursday, August 12")))
assert len(results)==30
proof={"tests":len(results),"passed":len(results),"cases":results,"sources":3,"observations":18,"cross_table_comparisons":12,
 "producer_verification_only":True,"independent_review":False,"family_accepted":False,
 "repair_history":["Initial optional bs4 import unavailable; used existing lxml.","Initial text materialization normalized CRLF; exact originals restored from JSON and all hashes checked before extraction.","First parser run rejected publication header due overescaped regex; repaired escaping, then all eighteen observations extracted without changing source or retained values.","Wrong-caption negative test initially changed parent text while retaining the original child caption; corrected the fixture to remove the caption contents."],
 "parser_sha256":sha256(pathlib.Path(__file__).with_name("cpi_parser_v9.py").read_bytes()).hexdigest()}
(p/"parser-proof.json").write_text(json.dumps(proof,indent=2)+"\n")
print(json.dumps({k:v for k,v in proof.items() if k not in ["cases","repair_history"]}))

