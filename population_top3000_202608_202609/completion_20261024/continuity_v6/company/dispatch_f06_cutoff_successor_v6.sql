-- Root-only dispatch; no code installation, HTTP, source admission or data acceptance.
BEGIN;
SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='90s';
DO $dispatch$
DECLARE
 assignment_text text:=$assignment${
  "assignment_id": "continuity-v6-f06-ten-case-cutoff-source-bridge",
  "assigned_executor": "automation:6adc2c96db348191bca7cf4eacfdac32",
  "owner_id": "top3000-completion-20261024-root-5db48b32d103",
  "owner_fence": 3,
  "stop_generation": 1,
  "scope_sha256": "8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd",
  "authority_event_id": 9528,
  "not_after": "2026-10-24T12:15:00Z",
  "exclusive_component": "PROMPT02_F06_TEN_EXACT_CASE_CUTOFF_BRIDGES_V6",
  "root_will_not_concurrently_modify": true,
  "producer_role": "Prompt02 company adjudication; source extraction may use delegated Financial04 role; Prompt02 alone proposes company membership or rank",
  "independent_reviewer_role": "Distinct numerical/source/temporal reviewer continuity_review or another explicitly authorized reviewer, never producer",
  "concrete_required_output": "Consume the exact29 committed retained-source facts and reviewed10-case gap ledger. Reconcile available retained additional originals first, including the two original held facts. Produce independently reviewed dated common-share/class/ADS bridge components and exact executable source recovery requests for gaps; no full family or rank acceptance by this assignment.",
  "allowed_code_files": [
    "continuity_v6/company/build_f06_cutoff_gap_v6.py",
    "continuity_v6/company/test_f06_cutoff_gap_v6.py",
    "continuity_v6/company/f06_ten_case_bridge_v6.sql",
    "continuity_v6/company/f06_ten_case_bridge_rollback_v6.sql",
    "continuity_v6/company/f06_typed_retained_source_import_v6.sql",
    "continuity_v6/company/f06_typed_retained_source_import_rollback_v6.sql",
    "continuity_v6/company/f06_ten_case_bridge_source_manifest_v6.json"
  ],
  "allowed_database_objects": [
    "Only exact allowed_code_objects newly created private objects in mission schema",
    "Use existing source_document, pipeline_task, pipeline_event, pipeline_commit, continuation_clear_queue_v6 via reviewed fenced interfaces; do not replace those schemas or helpers"
  ],
  "protected_invariants": [
    "Do not edit existing F06 source-fact definitions or repin17 installed source admissions",
    "Original31fact target,30HTMLtarget,5910company denominator and all714unresolved cases preserved",
    "Exact10current case hashes must match before integration; changes require reconciliation and independent review",
    "No parent case head update or membership/rank decision until separately reviewed complete Prompt02 proposal",
    "Keep12post-rank-publication facts explicitly later; no original receipt inferred",
    "Do not turn rounded values into exact counts; no interpolation across August3; arithmetic residual not treasury without source context",
    "ASML IR body must retain issuer provenance; ASIC PDF must retain regulator provenance; neither may be imported as SEC",
    "Private source bytes and raw source spans stay out of publicGit",
    "No unknown external outcome retry or expired lease stealing",
    "Preserve all attempts,counters,leases,source archives and originaldeadline; obey later stops and ownerchange"
  ],
  "input_versions": {
    "gap_manifest_sha256": "92b04d39b703dacbc500c12eff166039811bf6c1b02ee8129a4a03f80ce7684b",
    "private_gap_ledger_sha256": "1bc7bab781411b10a0f3f1d0e139c1e44f42f30f88606e13c39e78e0f1b5506b",
    "private_gap_input_sha256": "ff97d8957ad77201ce0d5040eaf1b0371679888f20da7d0dbdbea4d5d308eb27",
    "gap_independent_review_sha256": "391540a6256216e18838723aed0b33b9181cafa2d69b6f77aadb64a48b970f46",
    "f06_useful_commit_receipt_sha256": "f880f2201597b3286bf58cfd92c7d9b8fd98305a39d268e56decd33aa7d55874",
    "source_discovery_sha256": "a65cc29f11ebc8ea6cec22d9bd213fde3caf6dc43d5e754ff5c6ff44356cb6ac",
    "source_packet_sha256": "8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca",
    "f06_bundle_sha256": "9bdf83192df1023cbf86fc1420b9bec3f523fcb5e4a894ef9dc72fd4a781651c",
    "registration_event_id": 18875,
    "review_reuse_event_id": 18881,
    "case_pins": [
      {
        "case_key": "CIK:0000937966",
        "expected_parent_sha256": "087945fb161bca7308f2012a7c3b92c3aa4ae955c9a08a1f529b115fdf184947",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:9",
            "record_sha256": "973edb07f84fe0b98d57054e499e09f4ca11acc10387e5a57d15416daf5020da",
            "source_sha256": "cf8c4d06599938a3fd9478bdf25aa19e0f569637b87d9697d48f12e8bb1f1cb6"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:10",
            "record_sha256": "6eea3dc980810ec8a6c6976da17e40f5b27e18cb8032d86518c86d1394ec6a6b",
            "source_sha256": "cf8c4d06599938a3fd9478bdf25aa19e0f569637b87d9697d48f12e8bb1f1cb6"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0000937966:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "Exact unrounded common shares at August 3 close and transaction bridge",
          "Preserve registered-share listing form; do not infer an ADS ratio",
          "Separate issuer-IR provenance correction for retained buyback body"
        ],
        "requirement_sha256": "f8dc856e85bf9672f514f99055ebcac1ff609b05abab35b897771cd97e35bd42"
      },
      {
        "case_key": "CIK:0001046179",
        "expected_parent_sha256": "360da375204aed7b76d8fe2f13b593f1a7b0884a9cf41e52b2306e62aea650f2",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:12",
            "record_sha256": "f2e2f5fc91bf83d1985baff3a1a965a6ab912c0a8d1c761d21ebd145b895981e",
            "source_sha256": "169a1300ab2e7a2005934ee73f41496065204775d9d791b3ddc3b46e8f5848c3"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:13",
            "record_sha256": "5bbf986707c52998463d2481e05c703d5d7e65c814ef457de56aa2a32d50ef66",
            "source_sha256": "d4e0a0ae9c0f6578ececa50983c4295d9b2538cacb5519b9b73da7c64e921102"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0001046179:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "Exact unrounded ordinary shares, treasury and dated ADS ratio at August 3 close",
          "June 30 to August 3 changes; rounded millions/thousands are not an exact share count"
        ],
        "requirement_sha256": "f5885c70ca518c58ba837b31dca846691e698c7f4f06dfc99ed21d77547499a0"
      },
      {
        "case_key": "CIK:0001089113",
        "expected_parent_sha256": "2663a8ca0f6f79fe142588b9ed6e3ba0bda32e558f5eea9d6254e14bd77cb86b",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:6",
            "record_sha256": "673a4427a1e0eec6f9bba77a4a70c0a253c9cf2e8e369b50add64ae2ed5fbb62",
            "source_sha256": "95154451c3e9099c68756ac16818872fbf7f71579dc6b89ff2f0eb3bd82b162d"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:7",
            "record_sha256": "4308dda78911bac3abf2c8ad07ae09d575b3080918b901d6d8577ce3e0d050b5",
            "source_sha256": "38ee092486462bf3b93556809d6053b1580fb3bfb1c1bea0fcc2a2cc392258c5"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0001089113:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "July 30 voting-rights baseline to August 3 close issued and treasury movements",
          "Dated five-ordinary-per-ADS continuity and complete global ordinary valuation"
        ],
        "requirement_sha256": "8523fb7a6c279698ebd5c49cba169c0e69cf5df784e8d7b25a55425f9506503e"
      },
      {
        "case_key": "CIK:0001290877",
        "expected_parent_sha256": "4f9742ea54ca6e65c23bb2523c81dc8a220036856d83eb1acd064f36ca6c7e1b",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:25",
            "record_sha256": "14ebff31587cfc0fd40d1023e33f75e004b040fd83faa70581fac99ae393f190",
            "source_sha256": "0eac5491f0d41dd1eea41f7c0ed4ad4bc32741f3289ade9681940485368f76bb"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:26",
            "record_sha256": "65b26bf01e145586e3e9f858894b27cab1eedcf1a2663cfd6669b8a804dfc569",
            "source_sha256": "57aae00ff68bdc12e134397c623ed01f38c3ec895626bf0d8ade2d7c5314ba3b"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0001290877:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "May 31 to August 3 ordinary and treasury changes",
          "Dated one-ordinary-per-ADS continuity and global ordinary coverage"
        ],
        "requirement_sha256": "09d7c0bf9aff332a7e92b479366d513895394a3da185b201fbed7cf0d906708f"
      },
      {
        "case_key": "CIK:0001510593",
        "expected_parent_sha256": "6652586b02b8dc996b48d880149938ebd70bce8bbd746b43566cbaf733fd12ef",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:21",
            "record_sha256": "ddcfd353d01dd06ee77cd21acaae3d7f7ac14f34efc476e59545c84fd138b205",
            "source_sha256": "7d917d37467d1690fceb3207eb9a320038a85938b7de60e45a99047627a844d7"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:22",
            "record_sha256": "fe1882e2a4fb8d45bb73b6bdf52e24233dcd7b0275545c3fae805d58f3cc7762",
            "source_sha256": "7d917d37467d1690fceb3207eb9a320038a85938b7de60e45a99047627a844d7"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:28",
            "record_sha256": "12c5806682bccdb55b42395b2f46cd09f6960d21b72a101a5ada0b76c096d8b4",
            "source_sha256": "79cdc38d1935c72ec075a8276643e3bceac2e789b53d02e2fa0633aa9457f8bc"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0001510593:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "June 30 to August 3 ordinary, treasury and corporate-action bridge",
          "Dated five-ordinary-per-ADS continuity and global common-equity scope"
        ],
        "requirement_sha256": "df81998f2f436d1b290ac755dfea69b73d848196034e781eb5ddffc2471d54e3"
      },
      {
        "case_key": "CIK:0001577552",
        "expected_parent_sha256": "ad8714571cbc734fc78e951c71216b938c8febe07ea97f2f31ad8ae32c68feba",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:8",
            "record_sha256": "3050dfc2c315ac29249e79f9c08ffa469465b446ca87458b46f19d78695ae3fb",
            "source_sha256": "d5003622c4eb2da3fe784d166b9959645547498388e51d0a1e9bbf69e6109e66"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0001577552:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "July 31 to August 3 close ordinary and treasury movements",
          "Dated ADS ratio and complete global ordinary coverage; preserve August 6 publication"
        ],
        "requirement_sha256": "32bf26b27e16d36e1dd923d1b61550f603ed306f86e971657cb1930cb7935023"
      },
      {
        "case_key": "CIK:0001643303",
        "expected_parent_sha256": "f21277c2420985a11f59cddba17fcc935fdc931348d1771062568dc889b7c116",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:14",
            "record_sha256": "adce4858d03a24516d7dffaafa29994d0cfc7475bc4c0cccef0357717c49300a",
            "source_sha256": "d385ad2c242e8548143c64d1749c15cfac4fd1037da8e0d4dee8417b6c572f8a"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:15",
            "record_sha256": "bdf7ab2595c765a6fdd0cd9f63ef05d6de5143db011de8a56e0da6a7c55d64aa",
            "source_sha256": "d385ad2c242e8548143c64d1749c15cfac4fd1037da8e0d4dee8417b6c572f8a"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:16",
            "record_sha256": "f4d1879bf8f2040ffd109755c3cf8d302129c3107e81fe14450b59f217fc85a0",
            "source_sha256": "d385ad2c242e8548143c64d1749c15cfac4fd1037da8e0d4dee8417b6c572f8a"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:17",
            "record_sha256": "a3beab159c249e3d1c449ca2ad1f51748d91dfb7e861d1dc162fc71f774ff9d6",
            "source_sha256": "d385ad2c242e8548143c64d1749c15cfac4fd1037da8e0d4dee8417b6c572f8a"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:18",
            "record_sha256": "d15d3e1b968c71f6ec8714123f132c9a575e0e0c12027e3d3e9e5395849bc900",
            "source_sha256": "d385ad2c242e8548143c64d1749c15cfac4fd1037da8e0d4dee8417b6c572f8a"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0001643303:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "Allocate June 30 to August 5 share changes to dated events around August 3 close",
          "Dated ADS ratio continuity; zero repurchases does not prove zero issuance",
          "Separate issued-minus-outstanding arithmetic from treasury classification"
        ],
        "requirement_sha256": "02237cf89b3cb9a85d0c07ace0e0d7a0cb657b15a9a48d4d650f401145f91915"
      },
      {
        "case_key": "CIK:0001802883",
        "expected_parent_sha256": "55acd73a91d3efebae4b8a8b4d539dd5b2db63b6cf540b530a382f08a2c84313",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:23",
            "record_sha256": "575ce25fec04fc8cb9142bf3bb8489231af3f7665f9a473581a33c0365be5077",
            "source_sha256": "e0469b9c0d2ee5758c0133fcc78b80bd751823e0d0e50a374020338943d886a7"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:24",
            "record_sha256": "67625c7c3c0961497745a170f66637ca64a91e34d5c35d269934a8a1309e3598",
            "source_sha256": "e0469b9c0d2ee5758c0133fcc78b80bd751823e0d0e50a374020338943d886a7"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:29",
            "record_sha256": "9ed51bc0cda2fe8f5efd55a55cb8f2dbd1e4e88714fca44c5bcfd48d970868f1",
            "source_sha256": "1db0d642997156da5cb7c14e884588c4c8f03451cb545f45fab288cac59a9cee"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:30",
            "record_sha256": "ba293cf77708d82896f75d4a5b7acd742261fe4a5b3f1a5359e47bbdf8a823f2",
            "source_sha256": "1db0d642997156da5cb7c14e884588c4c8f03451cb545f45fab288cac59a9cee"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0001802883:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "Exact August 3 class A/B shares and class valuation equivalence",
          "December 31 class totals and June 30 rounded aggregate require separate dated changes",
          "Dated four-class-A-per-ADS continuity and class B economic rights"
        ],
        "requirement_sha256": "63c703fe49a87520268382bd341b5fcc31f1df44cec587d8629682f2528b5713"
      },
      {
        "case_key": "CIK:0002120882",
        "expected_parent_sha256": "f9db3157edbe8f004e1d4c76b714faa431cb0dd89e1fe8d0593efdb584b77b66",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:1",
            "record_sha256": "7054bbb0e0ccf847890801c7d42fd9878aa20a9a2c29d2ecc06c1d8b8d6df4af",
            "source_sha256": "7f51379006e32569ccf8605095d73a140c4b5b96465258a79e6f94c713bc5ff5"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:2",
            "record_sha256": "d14a585001c3b1cbddd2ab03abfced78e019f9e9df64c35cd2ff2b104a33f450",
            "source_sha256": "7f51379006e32569ccf8605095d73a140c4b5b96465258a79e6f94c713bc5ff5"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:3",
            "record_sha256": "815804be7ad9e3b80a6e3cd4abf506cf1771aaf5606392f67ac0f757cad33dcb",
            "source_sha256": "7f51379006e32569ccf8605095d73a140c4b5b96465258a79e6f94c713bc5ff5"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:4",
            "record_sha256": "b8deb1a31c55a0537223d7f68fb63fe112d35f573b2fa29b7c6b81cc4fe2864d",
            "source_sha256": "326b79cdde86f84af1c658b587ed44a2db1268c7abc76c6aefef374665275118"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:5",
            "record_sha256": "357af6e6d380520ad346b60df824566244a2f0c1e7fad710e5e3da16dfcb1b1e",
            "source_sha256": "326b79cdde86f84af1c658b587ed44a2db1268c7abc76c6aefef374665275118"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0002120882:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "Exact July 14 issuance-to-August 3 close changes, including employee issuance and treasury movements",
          "Confirm ordinary-per-ADS ratio and complete ordinary capital scope at August 3 close"
        ],
        "requirement_sha256": "8633ed6f6ee9a4b83c9ab6485776eb5ab3fe49a78541e4fcc940d95e04516798"
      },
      {
        "case_key": "CIK:0002124403",
        "expected_parent_sha256": "6c0cbce3949525c19f3398ee00bc355b629ef6109eab5171d88925b776dcace2",
        "facts": [
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:19",
            "record_sha256": "60ca974dfadee42660873b065707a15bc47c2e729e1a99a50e684520cace84fc",
            "source_sha256": "0b64447aafe963b7218b0d98706c3123f1fa04895a4e483129e84ced9deca6b5"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:20",
            "record_sha256": "d21074d02254b0f90b2893d92479001a87139300cf3387522a7669af77556845",
            "source_sha256": "0b64447aafe963b7218b0d98706c3123f1fa04895a4e483129e84ced9deca6b5"
          },
          {
            "fact_key": "f06-source-fact-v1:8952a160e5e46363dffd8d7932f0e78bc6e5af918dd8e7ffa930217bb86ed5ca:27",
            "record_sha256": "5e1da036a7b3deac6e396584f571337592c27e85754d72d6253d791fe6677a3a",
            "source_sha256": "64af6025ff01689f42eec1aa2831cde19ee8ad861ccd97361124c986dd574726"
          }
        ],
        "request_key": "ref01-v3:ref01-company-adjudication-v3-20261023:CIK:0002124403:ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION",
        "required_evidence": [
          "June 30 to August 3 ordinary, treasury and corporate-action bridge",
          "Dated ten-ordinary-per-ADS continuity; equal May/June endpoints do not prove a later unchanged state"
        ],
        "requirement_sha256": "066aef1858ad802dec54564de183be5ce19254c2ee1d9dcb8d7868fabf0ad908"
      }
    ]
  },
  "retained_first_execution": {
    "metadata_originals_discovered": 41,
    "metadata_original_bytes": 35515931,
    "already_integrated_originals": 17,
    "additional_metadata_originals": 24,
    "maximum_retained_bodies_to_inspect": 43,
    "maximum_retained_input_bytes": 50000000,
    "original_holds": [
      {
        "fact_ordinal": 11,
        "source_kind": "ISSUER_IR_HTML",
        "local_path": "financial04/ASML_buyback_20260810.htm",
        "source_sha256": "dddcc0dde1fbe96a1f75051113aae4af99154eb0afc44f728a5fde2825db4a5e",
        "source_bytes": 102793,
        "required_repair": "Provider and manifest version; preserve trade date versus settlement/treasury ambiguity"
      },
      {
        "fact_ordinal": 31,
        "source_kind": "ASIC_PDF",
        "local_path": "financial04/ASIC_total_issue_20260803.pdf",
        "source_sha256": "7c9a4c3453cacea775773aa6dd78c49db9fdecafda9fadf81d9c2212efde38b5",
        "source_bytes": 231853,
        "required_repair": "OriginalPDF retention with exactbinary hash and independently verified page/table extraction; issued capital is not automatically common outstanding"
      }
    ]
  },
  "finite_bounds": {
    "maximum_elapsed_minutes": 30,
    "maximum_tool_calls": 80,
    "maximum_repair_iterations": 2,
    "maximum_production_integrations": 1,
    "maximum_transient_lock_retries": 2,
    "maximum_source_requests": 20,
    "maximum_http_invocations": 20,
    "maximum_response_bytes_each": 8388608,
    "maximum_new_source_bytes": 67108864,
    "maximum_new_storage_bytes_including_staging": 134217728,
    "maximum_new_tasks": 40,
    "maximum_concurrency": 1,
    "minimum_request_interval_ms": 1000,
    "maximum_request_runtime_seconds": 30
  },
  "source_admission_condition": "The20requests are a proposal ceiling, not a reservation. Before any HTTP, exactURLs+provider+case/requirement must be independently reviewed and reserved via existing guarded source/direct-connector admission under currentcontrols and sharedstoragecap; measure retaineddiscoverability first. No blanket SECsubmissions fetch. Unknownresponses preserve leases and block only affectedrequests.",
  "tests": [
    "Reproduce reviewed gapledger and negativecases with exactinputpins",
    "Independently verify everynewsourcebodyhash, numerical span, date and fullclasscoverage",
    "ActualPostgreSQLrollback for anynewimport/helper; test staleparent, duplicate, bytecap, counterpreservation, ownerchange and stoprejection",
    "Taskreplay must produce zero newrows; exactcommits must emit independentverification and exactmissing-source successors",
    "Re-run only changedrisks; reuse17source review/runtime proof"
  ],
  "expected_useful_commit": "Immutable independently reviewed source-backed bridge components or precise unfilled source-condition receipts for each of10exactcase requirements, with input/output hashes and actual source/body commit IDs; READY alone is insufficient",
  "expected_successor": "At most10 exactsource acquisition clearing tasks plus10 independentnumerical/temporalreview tasks, followed onlyby reviewed Prompt02caseproposal; currentF06validationhold remains until requirement actuallyproved",
  "next_assignment_rule": "Only remaining named gap in these10cases with exact updatedinputpins and freshfiniteadmission. A source requiring a different provider/provenance route uses a separatelyversioned reviewed request. No expansion to othercases or family acceptance.",
  "final_acceptance_authority": "ROOT_ONLY",
  "status": "PINNED_CONCRETE_SUCCESSOR_PENDING_ROOT_DISPATCH",
  "repository": "robert8p/market-data-leading-indicator-lab",
  "parent_authority_event_ids": [
    17776,
    18875,
    18881
  ],
  "allowed_code_objects": [
    "f06_ten_case_bridge_input_v6",
    "f06_ten_case_bridge_component_v6",
    "f06_ten_case_bridge_manifest_v6(jsonb)",
    "f06_ten_case_bridge_admit_v6(text,bigint,bigint,jsonb)",
    "f06_ten_case_bridge_derive_v6(uuid,text)",
    "f06_ten_case_bridge_validate_v6(uuid,text)",
    "f06_ten_case_bridge_source_successors_v6(text,bigint,bigint,text)",
    "f06_typed_retained_source_chunk_v6",
    "f06_typed_retained_source_import_v6(text,bigint,bigint,jsonb)"
  ],
  "code_commit": "c109a6241f262b749e58e428faa918caf8d87579",
  "maximum_elapsed_minutes": 30,
  "maximum_tool_calls": 80,
  "maximum_repair_iterations": 2,
  "maximum_production_integrations": 1,
  "maximum_transient_lock_retries": 2,
  "maximum_source_requests": 20,
  "maximum_http_invocations": 20,
  "maximum_response_bytes_each": 8388608,
  "maximum_new_source_bytes": 67108864,
  "maximum_new_storage_bytes_including_staging": 134217728,
  "maximum_new_tasks": 40,
  "maximum_concurrency": 1,
  "minimum_request_interval_ms": 1000,
  "maximum_request_runtime_seconds": 30
}
$assignment$;
 review_text text:=$review${
  "reviewer": "continuity_review",
  "producer": "company_resume",
  "approved": true,
  "reviewed_at": "2026-10-24T09:49:59.148667+00:00",
  "decision": "APPROVED_FINITE_EXACT_TEN_CASE_SUCCESSOR_DISPATCH",
  "assignment_id": "continuity-v6-f06-ten-case-cutoff-source-bridge",
  "assignment_artifact_sha256": "586296bb726cf1e1f140ec68aa7ba4879cba0c19349740fc37386a5381f7e3d7",
  "code_commit": "c109a6241f262b749e58e428faa918caf8d87579",
  "repository": "robert8p/market-data-leading-indicator-lab",
  "parent_authority_event_ids": [
    17776,
    18875,
    18881
  ],
  "input_gap_review_sha256": "391540a6256216e18838723aed0b33b9181cafa2d69b6f77aadb64a48b970f46",
  "current_case_heads_independently_verified": 10,
  "actual_retained_source_fact_hashes_independently_verified": 29,
  "source_discovery_is_metadata_only_for_uninspected_bodies": true,
  "held_originals_independently_rehashed": {
    "ASML_issuer_HTML": {
      "bytes": 102793,
      "sha256": "dddcc0dde1fbe96a1f75051113aae4af99154eb0afc44f728a5fde2825db4a5e"
    },
    "ASIC_original_PDF": {
      "bytes": 231853,
      "sha256": "7c9a4c3453cacea775773aa6dd78c49db9fdecafda9fadf81d9c2212efde38b5"
    }
  },
  "maximum_retained_bodies": 43,
  "maximum_retained_input_bytes": 50000000,
  "proposed_finite_bounds": {
    "maximum_elapsed_minutes": 30,
    "maximum_tool_calls": 80,
    "maximum_repair_iterations": 2,
    "maximum_production_integrations": 1,
    "maximum_transient_lock_retries": 2,
    "maximum_source_requests": 20,
    "maximum_http_invocations": 20,
    "maximum_response_bytes_each": 8388608,
    "maximum_new_source_bytes": 67108864,
    "maximum_new_storage_bytes_including_staging": 134217728,
    "maximum_new_tasks": 40,
    "maximum_concurrency": 1,
    "minimum_request_interval_ms": 1000,
    "maximum_request_runtime_seconds": 30
  },
  "HTTP_authorized_by_this_review": false,
  "source_admission_requirement": "Before any HTTP exactURLs/provider/case requirements must be independently reviewed and reserved under current shared admission;20request ceiling is not a reservation. Retained originals first.",
  "allowed_code_objects": [
    "f06_ten_case_bridge_input_v6",
    "f06_ten_case_bridge_component_v6",
    "f06_ten_case_bridge_manifest_v6(jsonb)",
    "f06_ten_case_bridge_admit_v6(text,bigint,bigint,jsonb)",
    "f06_ten_case_bridge_derive_v6(uuid,text)",
    "f06_ten_case_bridge_validate_v6(uuid,text)",
    "f06_ten_case_bridge_source_successors_v6(text,bigint,bigint,text)",
    "f06_typed_retained_source_chunk_v6",
    "f06_typed_retained_source_import_v6(text,bigint,bigint,jsonb)"
  ],
  "provenance_requirements": [
    "IssuerHTML remains issuer provenance, originalPDF remains regulator provenance; never mislabeledSEC",
    "Original31/30facts and all company denominators preserved",
    "No rank-close interpolation or arithmetic residual treasury inference",
    "Append new version outputs, keep old17source admissions unchanged"
  ],
  "company_membership_or_rank_acceptance": false,
  "family_acceptance": false,
  "dispatch_requirements": "Root current authority and overlap checks; append exact immutable V5 assignment and executable queue. Recheck current case/input pins before integration; independent review and actual rollback for changed implementation.",
  "committed_gap_manifest_independently_fetched_and_byte_matched": true,
  "claim_api_compatibility_delta": "Exact finite_bounds values duplicated at top level for existing V5 claim SQL; all values unchanged, no additional authority."
}
$review$;
 assignment jsonb:=assignment_text::jsonb;review jsonb:=review_text::jsonb;
 assignment_sha text:='42add4e9c7187e51a9e3c530494d95557c6a10e16b63cb2e41d0bab9bc00e17c';
 clearing_key text:='f06-ten-case-cutoff-v6:42add4e9c7187e51a9e3c530494d95557c6a10e16b63cb2e41d0bab9bc00e17c';
 prior_key text:='0fe77dc18de981e3bb8016accd9ae3f94aea6ae96c4dc71ff230caff24571c32';
 prior_hash text:='b5e4eb3a025ac2318d2086a20627f25a1ea2154ea18bc70f6f518006f650459e';
 ctl jsonb;before_ac jsonb;before_pc jsonb;manifest jsonb;manifest_hash text;prior_assignment jsonb;
 ae bigint;re bigint;pin jsonb;fpin jsonb;facts_checked integer:=0;
 prior population_top3000_202608_202609_v1.continuation_clear_queue_v6%rowtype;
BEGIN
 IF encode(sha256(convert_to(assignment_text,'UTF8')),'hex') IS DISTINCT FROM '586296bb726cf1e1f140ec68aa7ba4879cba0c19349740fc37386a5381f7e3d7'
 OR encode(sha256(convert_to(assignment::text,'UTF8')),'hex') IS DISTINCT FROM assignment_sha
 OR encode(sha256(convert_to(review_text,'UTF8')),'hex') IS DISTINCT FROM '173f0184da0a96bff74a79b0f5e66011705399435d0f6cb454a740e815ffc795'
 OR review->>'approved' IS DISTINCT FROM 'true' OR review->>'reviewer' IS DISTINCT FROM 'continuity_review'
 OR review->>'producer' IS DISTINCT FROM 'company_resume'
 OR review->>'assignment_artifact_sha256' IS DISTINCT FROM encode(sha256(convert_to(assignment_text,'UTF8')),'hex')
 OR review->>'code_commit' IS DISTINCT FROM assignment->>'code_commit'
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_EXACT_REVIEWED_ARTIFACT_REQUIRED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.continuation_guard_v6(text,bigint,bigint)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '718fdc547db75a78b8bea8e8ed1c0afe42d9de0b07702aeb24fc01d27fe4c06d'
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_CURRENT_GUARD_CHANGED';END IF;
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT to_jsonb(a) INTO before_ac FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton;
 SELECT to_jsonb(p) INTO before_pc FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton;
 IF assignment->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR assignment->>'owner_fence' IS DISTINCT FROM ctl->>'owner_fence'
 OR assignment->>'stop_generation' IS DISTINCT FROM ctl->>'stop_generation' OR assignment->>'scope_sha256' IS DISTINCT FROM ctl->>'scope_sha256'
 OR assignment->>'authority_event_id' IS DISTINCT FROM '9528' OR assignment->>'root_will_not_concurrently_modify' IS DISTINCT FROM 'true'
 OR (assignment->>'not_after')::timestamptz> (ctl->>'not_after')::timestamptz OR (assignment->>'not_after')::timestamptz<=clock_timestamp()
 OR before_pc->>'deadline' IS DISTINCT FROM '2026-10-25T22:36:17.687194+00:00'
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_CURRENT_AUTHORITY_OR_ORIGINAL_DEADLINE_CHANGED';END IF;
 manifest:=jsonb_build_array(jsonb_build_object('kind','F06_EXACT_TEN_CASE_CUTOFF_SUCCESSOR_V6','assignment_id',assignment->>'assignment_id',
 'assignment_details_sha256',assignment_sha,'code_commit',assignment->>'code_commit','case_pins',assignment#>'{input_versions,case_pins}',
 'gap_manifest_sha256',assignment#>>'{input_versions,gap_manifest_sha256}',
 'parent_clearing_id',prior_key,'clearing_input_sha256',prior_hash,'required_action',assignment->>'concrete_required_output'));
 manifest_hash:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
 SELECT e.event_id,e.details INTO ae,prior_assignment FROM population_top3000_202608_202609_v1.pipeline_event e
 WHERE e.event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5' AND e.details->>'assignment_id'=assignment->>'assignment_id'
 ORDER BY e.event_id LIMIT 1;
 IF ae IS NOT NULL THEN
  IF prior_assignment IS DISTINCT FROM assignment OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 q
   WHERE q.clearing_id=clearing_key AND q.assignment_event_id=ae AND q.input_manifest=manifest AND q.input_sha256=manifest_hash
   AND q.owner_id=ctl->>'owner_id' AND q.owner_fence=3 AND q.stop_generation=1 AND q.scope_sha256=ctl->>'scope_sha256')
  THEN RAISE EXCEPTION 'F06_SUCCESSOR_REPLAY_CONFLICT';END IF;
  RETURN; -- exact replay never resets queue status, attempts or claims
 END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id IN('F06','REF01') AND status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE family_id IN('F06','REF01') AND status='CLAIMED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5'
 AND (e.details->>'exclusive_component' LIKE '%F06%' OR e.details->>'exclusive_component' LIKE '%REF01%' OR e.details->>'exclusive_component'=assignment->>'exclusive_component')
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event done WHERE done.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND done.details->>'claim_event_id'=e.event_id::text AND done.details->>'effects_known'='true'))
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_AFFECTED_LANE_OR_UNRECONCILED_CLAIM';END IF;
 IF population_top3000_202608_202609_v1.f06_span_bundle_v1() IS DISTINCT FROM assignment#>>'{input_versions,f06_bundle_sha256}'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=17776)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=18875)
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=18881)
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_PARENT_OR_CODE_PIN_CHANGED';END IF;
 IF jsonb_array_length(assignment#>'{input_versions,case_pins}')<>10 THEN RAISE EXCEPTION 'F06_SUCCESSOR_TEN_EXACT_CASES_REQUIRED';END IF;
 FOR pin IN SELECT value FROM jsonb_array_elements(assignment#>'{input_versions,case_pins}') LOOP
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_adjudication_head_v3 h WHERE h.run_key='ref01-company-adjudication-v3-20261023' AND h.case_key=pin->>'case_key' AND h.record_sha256=pin->>'expected_parent_sha256')
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_source_request_v1 q WHERE q.request_key=pin->>'request_key' AND q.kind='REF01_TARGETED_EVIDENCE_V3' AND q.payload->>'case_key'=pin->>'case_key' AND q.payload->>'base_case_sha256'=pin->>'expected_parent_sha256' AND q.payload->>'requirement_sha256'=pin->>'requirement_sha256')
  THEN RAISE EXCEPTION 'F06_SUCCESSOR_EXACT_CASE_REQUEST_PREIMAGE_CHANGED';END IF;
  FOR fpin IN SELECT value FROM jsonb_array_elements(pin->'facts') LOOP
   IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.f06_provisional_company_fact_v1 f WHERE f.fact_key=fpin->>'fact_key' AND f.record_sha256=fpin->>'record_sha256' AND f.source_sha256=fpin->>'source_sha256' AND f.company02_target_request->>'case_key'=pin->>'case_key')
   THEN RAISE EXCEPTION 'F06_SUCCESSOR_EXACT_SOURCE_FACT_PREIMAGE_CHANGED';END IF;
   facts_checked:=facts_checked+1;
  END LOOP;
 END LOOP;
 IF facts_checked<>29 THEN RAISE EXCEPTION 'F06_SUCCESSOR_EXACT_29_FACTS_REQUIRED';END IF;
 SELECT * INTO STRICT prior FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE clearing_id=prior_key FOR UPDATE;
 IF prior.status IS DISTINCT FROM 'READY' OR prior.attempts<>0 OR prior.claimed_at IS NOT NULL OR prior.claim_token_sha256 IS NOT NULL
 OR prior.input_sha256 IS DISTINCT FROM prior_hash OR prior.assignment_event_id IS DISTINCT FROM 19146
 OR prior.family_id IS DISTINCT FROM 'F06' OR jsonb_array_length(prior.input_manifest)<>17
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_GENERIC_CLEARING_PREIMAGE_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(prior.input_manifest) m LEFT JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=(m->>'task_id')::uuid
 WHERE t.task_id IS NULL OR t.family_id IS DISTINCT FROM 'F06' OR t.stage IS DISTINCT FROM 'VALIDATE' OR t.status IS DISTINCT FROM 'BLOCKED'
 OR t.blocker_code IS DISTINCT FROM prior.blocker_code OR m->'input_versions' IS DISTINCT FROM t.input_versions OR m->'checkpoint' IS DISTINCT FROM t.checkpoint
 OR m->>'payload_sha256' IS DISTINCT FROM encode(sha256(convert_to(t.payload::text,'UTF8')),'hex') OR m->>'executions' IS DISTINCT FROM t.executions::text OR m->>'cumulative_failures' IS DISTINCT FROM t.cumulative_failures::text)
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_VALIDATION_HOLD_PREIMAGE_CHANGED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 WHERE status NOT IN('COMPLETE','SUPERSEDED'))>=96
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_QUEUE_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('ENGINEERING_F06_CUTOFF_SUCCESSOR_REVIEW_V6',ctl||jsonb_build_object('assignment_id',assignment->>'assignment_id','assignment_details_sha256',assignment_sha,'independent_review_artifact_sha256','173f0184da0a96bff74a79b0f5e66011705399435d0f6cb454a740e815ffc795','independent_review',review,'no_source_admission',true)) RETURNING event_id INTO re;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5',assignment) RETURNING event_id INTO ae;
 INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,assigned_executor,not_after,assignment_event_id,last_receipt)
 VALUES(clearing_key,'COMPANY_RECONCILIATION','F06','F06_TEN_CASE_RETAINED_CUTOFF_BRIDGE_V6',manifest,manifest_hash,'READY',ctl->>'owner_id',3,1,ctl->>'scope_sha256',assignment->>'assigned_executor',(assignment->>'not_after')::timestamptz,ae,
 jsonb_build_object('independent_review_event_id',re,'replaces_unclaimed_generic_clearing_id',prior_key,'source_download_admission_granted',false,'retained_first',true,'pipeline_holds_unchanged',true));
 UPDATE population_top3000_202608_202609_v1.continuation_clear_queue_v6 SET status='SUPERSEDED',last_receipt=jsonb_build_object('effects_known',true,'implementation_executed',false,'reason','EXACT_REVIEWED_TEN_CASE_SUCCESSOR_DISPATCHED','successor_clearing_id',clearing_key,'successor_assignment_event_id',ae,'source_task_holds_preserved',true),updated_at=clock_timestamp() WHERE clearing_id=prior_key AND status='READY' AND attempts=0;
 IF NOT FOUND THEN RAISE EXCEPTION 'F06_SUCCESSOR_GENERIC_CLEARING_CAS_FAILED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_F06_CUTOFF_SUCCESSOR_DISPATCHED_V6',ctl||jsonb_build_object('assignment_event_id',ae,'assignment_id',assignment->>'assignment_id','assignment_details_sha256',assignment_sha,'clearing_id',clearing_key,'input_sha256',manifest_hash,'independent_review_event_id',re,'replaced_generic_clearing_id',prior_key,'prior_generic_assignment_event_id',19146,'prior_generic_input_sha256',prior_hash,'useful_data_completion',false,'source_requests',0,'source_admission_granted',false,'data_holds_preserved',true,'cumulative_counters_preserved',true));
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF before_ac IS DISTINCT FROM (SELECT to_jsonb(a) FROM population_top3000_202608_202609_v1.acquisition_control a WHERE singleton)
 OR before_pc IS DISTINCT FROM (SELECT to_jsonb(p) FROM population_top3000_202608_202609_v1.pipeline_control p WHERE singleton)
 THEN RAISE EXCEPTION 'F06_SUCCESSOR_CONTROL_COUNTER_OR_DEADLINE_MUTATION';END IF;
END;
$dispatch$;
SELECT q.clearing_id,q.status,q.assignment_event_id,encode(sha256(convert_to(e.details::text,'UTF8')),'hex') assignment_details_sha256,q.input_sha256,jsonb_typeof(q.input_manifest) input_manifest_type,q.attempts,q.not_after,e.at dispatched_at
FROM population_top3000_202608_202609_v1.continuation_clear_queue_v6 q JOIN population_top3000_202608_202609_v1.pipeline_event e ON e.event_id=q.assignment_event_id
WHERE q.clearing_id='f06-ten-case-cutoff-v6:42add4e9c7187e51a9e3c530494d95557c6a10e16b63cb2e41d0bab9bc00e17c';
COMMIT;
