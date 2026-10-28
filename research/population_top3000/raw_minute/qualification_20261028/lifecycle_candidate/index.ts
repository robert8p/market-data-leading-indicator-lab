// Candidate entrypoint; deployment requires exact native/worker review and admission.
import postgres from 'npm:postgres@3.4.5';
import {makeHandler} from './handler.ts';
Deno.serve(makeHandler((name)=>Deno.env.get(name),(url)=>postgres(url,{max:1,prepare:false,ssl:'require',connect_timeout:5,idle_timeout:30,max_lifetime:60,debug:false,onnotice:()=>{}})));
