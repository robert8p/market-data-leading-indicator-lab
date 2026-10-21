import fs from 'node:fs';
import vm from 'node:vm';
import {stripTypeScriptTypes,createRequire} from 'node:module';
import assert from 'node:assert/strict';
const require=createRequire(import.meta.url);const {SaxesParser}=require('./stream_parser/node_modules/saxes');
let src=fs.readFileSync(new URL('./stream_capture.ts',import.meta.url),'utf8').replace(/^import .*\n/,'').replace('export type','type').replace('export function','function');src=stripTypeScriptTypes(src,{mode:'transform'});
const c={SaxesParser};vm.createContext(c);vm.runInContext(src+';globalThis.factory=createCaptureParser',c);
if(!process.argv[2])throw new Error('Provide the retained Massive Apple 2023 filing path as argument; no network source is fetched.');
const raw=fs.readFileSync(process.argv[2],'utf8');
let baseline;
for(const size of [65536,137,1]){
 const rows=[];const p=c.factory(x=>rows.push(x));for(let i=0;i<raw.length;i+=size)p.write(raw.slice(i,i+size));const count=p.close();
 const val=JSON.stringify(rows);if(baseline)assert.equal(val,baseline);else baseline=val;
 assert.equal(count.facts,995);console.log(JSON.stringify({chunk_chars:size,...count,raw_chars:raw.length}));
}
if(process.argv[3])fs.writeFileSync(process.argv[3],baseline);
console.log('PASS: byte-identical fragments across large, irregular and one-character boundaries');
