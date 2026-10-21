import { SaxesParser } from "npm:saxes@6.0.0";
const X="http://www.xbrl.org/2003/instance", IX="http://www.xbrl.org/2013/inlineXBRL";
export type Fragment={kind:string,ordinal:number,node_id:string|null,context_ref:string|null,unit_ref:string|null,raw_fragment:string,namespace_attrs:string};
const attr=(s:string)=>s.replaceAll("&","&amp;").replaceAll('"',"&quot;").replaceAll("<","&lt;");
export function createCaptureParser(onFragment:(f:Fragment)=>void){
 const parser=new SaxesParser({xmlns:true});
 let buffer="",base=0,fed=0,depth=0,tagStart=0,facts=0,contexts=0,units=0;
 const active:any[]=[];const scopes:Record<string,string>[]=[];
 parser.on("doctype",()=>{throw new Error("STREAM_DOCTYPE_UNSUPPORTED");});
 parser.on("error",(e:Error)=>{throw e;});
 parser.on("opentagstart",()=>{
  const local=buffer.lastIndexOf("<",parser.position-base-1);
  if(local<0)throw new Error("STREAM_TAG_BOUNDARY_LIMIT");tagStart=base+local;
 });
 parser.on("opentag",(tag:any)=>{
  depth++;const ns={...(scopes[scopes.length-1]||{}),...tag.ns};scopes.push(ns);
  let kind:string|null=null;
  if(tag.uri===X&&tag.local==="context")kind="CONTEXT";
  if(tag.uri===X&&tag.local==="unit")kind="UNIT";
  if(tag.uri===IX&&tag.local==="nonFraction")kind="FACT";
  if(kind){
   const get=(n:string)=>tag.attributes[n]?.value??null;
   const ordinal=kind==="FACT"?++facts:kind==="CONTEXT"?++contexts:++units;
   active.push({kind,ordinal,depth,start:tagStart,node_id:get("id"),context_ref:get("contextRef"),unit_ref:get("unitRef"),namespace_attrs:Object.entries(ns).filter(([k])=>k!=="xml"&&k!=="xmlns").map(([k,v])=>`xmlns${k?":"+k:""}="${attr(v)}"`).join(" ")});
  }
 });
 parser.on("closetag",()=>{
  const current=active[active.length-1];
  if(current&&current.depth===depth){
   const raw=buffer.slice(current.start-base,parser.position-base);
   if(raw.length>1024*1024)throw new Error("STREAM_FRAGMENT_REQUIRES_LARGER_LANE");
   const {kind,ordinal,node_id,context_ref,unit_ref,namespace_attrs}=current;
   onFragment({kind,ordinal,node_id,context_ref,unit_ref,namespace_attrs,raw_fragment:raw});active.pop();
  }
  scopes.pop();depth--;
 });
 function write(s:string){
  buffer+=s;fed+=s.length;parser.write(s);
  if(active.length&&buffer.length>2*1024*1024)throw new Error("STREAM_FRAGMENT_REQUIRES_LARGER_LANE");
  const keep=active.length?active[0].start:Math.max(base,fed-65536);
  buffer=buffer.slice(keep-base);base=keep;
 }
 function close(){parser.close();if(active.length||depth!==0)throw new Error("STREAM_XML_INCOMPLETE");return {facts,contexts,units};}
 return {write,close};
}
