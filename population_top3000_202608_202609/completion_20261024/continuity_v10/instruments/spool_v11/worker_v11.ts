import postgres from "npm:postgres@3.4.5";
import {buildWorker} from "./core_v11.ts";
Deno.serve(buildWorker({postgres,fetch,DenoEnv:(name:string)=>Deno.env.get(name)}));
