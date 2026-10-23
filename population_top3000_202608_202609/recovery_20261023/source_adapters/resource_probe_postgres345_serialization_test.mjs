// Uses unmodified official postgres.js v3.4.5 types/query/errors modules retained beside this test.
// No database, network connection, provider request or live credential is used.
// Server-assigned OIDs correspond to explicit ::jsonb (3802) and ::text (25) casts.
import assert from 'node:assert/strict';
import {Parameter,handleValue,inferType,serializers} from './driver_evidence/postgres-3.4.5/src/types.js';

const snapshot={outcome:'SUCCEEDED',observed_at:'2026-10-23T11:54:21.000Z',metrics_http_status:200,
 source_sha256:'b'.repeat(64),metrics:[{filesystem_id:'c'.repeat(64),mountpoint:'/fixture',available_bytes:40,free_bytes:50,size_bytes:100}],
 error_code:null,elapsed_ms:520,capabilities:{sec_user_agent_configured:false,sec_contact_email_configured:false,massive_key_configured:true}};
const options={transform:{undefined:undefined}};
function bindWithServerType(value,serverOid){
 const parameters=[];const types=[];
 assert.equal(handleValue(value,parameters,types,options),'$1');
 // Official connection.js ParameterDescription fills an unspecified OID before Bind.
 if(!types[0])types[0]=serverOid;
 const wire=types[0] in serializers?serializers[types[0]](parameters[0]):''+parameters[0];
 return {initial:inferType(value),type:types[0],wire,decoded:JSON.parse(wire)};
}
const broken=bindWithServerType(JSON.stringify(snapshot),3802);
assert.equal(broken.initial,0);assert.equal(broken.type,3802);
assert.equal(typeof broken.decoded,'string');
assert.equal(JSON.parse(broken.decoded).outcome,'SUCCEEDED');

// Official sql.json(x), also available on transaction scopes, constructs this exact Parameter.
const fixed=bindWithServerType(new Parameter(snapshot,3802),3802);
assert.equal(fixed.initial,3802);assert.equal(fixed.type,3802);
assert.equal(typeof fixed.decoded,'object');assert.deepEqual(fixed.decoded,snapshot);

// The explicit text-cast alternative also works, but the canonical json helper is preferred.
const textAlternative=bindWithServerType(JSON.stringify(snapshot),25);
assert.equal(textAlternative.initial,0);assert.equal(textAlternative.type,25);
assert.deepEqual(textAlternative.decoded,snapshot);
console.log('PASS_POSTGRES345_REAL_SERIALIZER_REPRODUCES_DOUBLE_ENCODING_AND_TYPED_JSON_FIX');
