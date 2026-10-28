// Candidate application adapter: no listener, deployment, timer or source side effect on import.
// Native claim/commit/failure RPCs still require implementation, tests and distinct review.
import {acquireRaw,WorkerError,type Manifest,type Reservation} from './raw_minute_transport.ts';
type Claim={state:string;manifest_text?:string;reservation?:Reservation};
type Dependencies={
 claim:(attempt:string,token:string)=>Promise<Claim>;
 commit:(attempt:string,token:string,page:unknown,reservation:Reservation)=>Promise<{state:string;receipt_recorded:boolean;raw_only:boolean}>;
 failure:(attempt:string,token:string,code:string,reservation:Reservation)=>Promise<{state:string;receipt_recorded:boolean}>;
 credential:()=>string;
 fetcher?:typeof fetch;
};
export async function runRawWorker(input:unknown,d:Dependencies){
 if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).sort().join(',')!=='attempt_id,token')return {state:'REJECTED',code:'INVALID_REQUEST',provider_requested:false};
 const {attempt_id,token}=input as {attempt_id:unknown;token:unknown};
 if(typeof attempt_id!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(attempt_id)||typeof token!=='string'||!/^[0-9a-f]{64}$/.test(token))return {state:'REJECTED',code:'INVALID_REQUEST',provider_requested:false};
 let claim:Claim;try{claim=await d.claim(attempt_id,token);}catch{return {state:'UNCONFIRMED',code:'CLAIM_OUTCOME_REQUIRES_RECONCILIATION',provider_requested:false};}
 if(claim?.state==='RAW_CHAIN_VERIFIED'&&(claim as any).receipt_recorded===true&&(claim as any).raw_only===true)return {state:'RAW_CHAIN_VERIFIED',receipt_recorded:true,raw_only:true,provider_requested:false,family_accepted:false};
 if(claim?.state!=='RESERVED')return {state:'NOT_RESERVED',provider_requested:false};
 const r=claim.reservation;let m:Manifest;
 try{if(typeof claim.manifest_text!=='string'||!r)throw Error();m=JSON.parse(claim.manifest_text);}catch{return {state:'UNCONFIRMED',code:'INVALID_RESERVED_CLAIM',provider_requested:false};}
 let credential:string;try{credential=d.credential();}catch{return {state:'UNCONFIRMED',code:'CREDENTIAL_UNAVAILABLE_RESERVATION_REQUIRES_RECONCILIATION',provider_requested:false};}
 let providerRequested=false;
 try{
  const receipt=await acquireRaw(m,r,credential,async(page,reservation)=>{
   const result=await d.commit(attempt_id,token,page,reservation);
   if(!result||result.receipt_recorded!==true||result.raw_only!==true||!['RAW_PAGE_COMMITTED','RAW_CHAIN_VERIFIED','RAW_CONTINUATION_REQUIRED'].includes(result.state))throw Error('UNCONFIRMED');
   return result;
  },async(code,reservation)=>{
   const result=await d.failure(attempt_id,token,code,reservation);
   if(!result||result.receipt_recorded!==true||result.state!=='SOURCE_FAILURE_RECORDED')throw Error('UNCONFIRMED');
  },async(url,init)=>{providerRequested=true;return await (d.fetcher||fetch)(url,init);});
  return {state:(receipt as {state:string}).state,receipt_recorded:true,provider_requested:providerRequested,normalized_values_created:0,family_accepted:false};
 }catch(e){
  // Never expose arbitrary RPC, transport, credential or parser exception text.
  const known=new Set(['TRANSPORT_OUTCOME_REQUIRES_RECONCILIATION','INVALID_TRANSPORT_RECEIPT_REQUIRES_RECONCILIATION','FAILURE_RECORD_OUTCOME_REQUIRES_RECONCILIATION','COMMIT_OUTCOME_REQUIRES_RECONCILIATION']);
  return {state:'UNCONFIRMED',code:e instanceof WorkerError&&known.has(e.code)?e.code:'SOURCE_OR_CONTROL_REQUIRES_RECONCILIATION',provider_requested:providerRequested,receipt_recorded:false};
 }
}
