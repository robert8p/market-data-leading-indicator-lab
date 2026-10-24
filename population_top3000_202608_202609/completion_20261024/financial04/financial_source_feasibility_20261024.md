# Financial source feasibility, 24 October 2026

Recorded at this resumed mission's current execution; these are source feasibility findings, not final family acceptance. Monetary approval is current and unlimited for necessary mission work. No old $250 restriction is applied. No purchase, paid entitlement activation, or vendor outreach has been performed by Prompt04. Current connector invoices are unavailable.

## Actual probes and retained execution

- **F05 historical consensus / earnings:** the actual connected Massive `/benzinga/v1/earnings` NVDA August–September request returned `NOT_ENTITLED` (1.343 seconds), retained in `initial_probe_evidence.json`. The current public Earnings product is $99/month for individual use; its API documents all history from 30 April 2010. Business licensing is contact-sales. This is an entitlement constraint, not a monetary cap.
- **F06 float:** the connected Massive `/stocks/vX/float` returned NVDA free float 23,150,460,000, effective 23 September 2026 (2.318 seconds). It supplies a latest snapshot, not August–September history. The official knowledge-base page now explicitly states that past-date parameters are ignored and every plan has no float history. This source cannot meet the full historical denominator by upgrading a plan.
- **SEC financial statements / company facts:** original SEC direct acquisition is functioning. CRD extracted XBRL was retained live, source hash `7a9176789eaf515caa6a53c8c803645744b3d0d2d8c9c1f08a0a254bf872630c`, 3,875,138 bytes, 1,335 source facts, 325 contexts, six units. Its source/index evidence preserves the after-close filing timing and leaves unverified canonical publication/receipt fields NULL. Additional original SEC company sources are retained locally with exact bytes/hashes and current retrieval receipts.
- No retained actual income-statements entitlement denial was found. An earlier unsupported inference was withdrawn. That endpoint's documentation concerns most-recent-restated filing data and does not itself establish original publication-time reconstruction.

## Bounded alternatives examined

| Required data | Actual evidence | Decision / remaining proof |
|---|---|---|
| Massive/Benzinga earnings | Current public price and self-service purchase link; actual connected entitlement denial. | Technically plausible for earnings calendar/actuals and reported estimate fields after activation. A current row with `last_updated` is not evidence of the preannouncement consensus vintage. Obtain a specific historical sample and revision semantics before F05 acceptance. |
| FactSet Estimates Point-in-Time Consensus | Official methodology describes daily snapshots at each primary market's local midnight, excluding estimates entered after the cutoff and later QA/currency/dilution revisions. Current FactSet-sold AWS listing advertises all historical revisions through a Redshift datashare and requires a FactSet ID. | Strong candidate for preannouncement daily consensus, with remaining sample test for exact fiscal periods, currencies, metrics, source count and time-cutoff semantics. The published methodology page is explicitly dated December 2019, so its current contract must be verified against delivered files. A midnight snapshot may be earlier than the last consensus immediately before an intraday announcement. |
| LSEG I/B/E/S | Official current catalogue advertises broker estimate values, sources, announcement dates, fiscal periods, currencies, unit scales and deep history; API/SFTP/cloud delivery. | Candidate archive, not accepted on catalogue wording. Require explicit point-in-time version product and actual samples; generic historical estimates may be revised. Access page offers a request-details/sales route. |
| EDI Free Float Service | Official service describes daily global float and separately purchasable annual history where available, over 170 exchanges, market-specific definitions and security identifiers. | Strong paid-history candidate. Need actual US/ADR/multiple-class coverage, applicable-date and publication/first-observation metadata, exact August–September sample, and delivered archive/license. Public route is contact/login, without an executable anonymous checkout. |
| FactSet Ownership | Official pages confirm float-related ownership information and broad historical ownership coverage. | Do not infer that all historical float fields are covered at the required frequency. Need primary current float-history field dictionary and sample; no delivered entitlement available. |
| FMP float | Official All Shares Float FAQ explicitly states only current-day float is supplied. | Rejected for missing historical requirement. |
| SEC-API public float | Official dictionary defines publicFloat as disclosed dollar amount, typically an annual cover disclosure; outstandingShares is separate. | Useful source-navigation/filing extraction alternative, not genuine historical free-float shares. Do not substitute dollar float or outstanding shares, or treat vendor advice to divide by price as a certified required float series. |
| FTSE/Russell index float | Official subscription page offers index constituents, free-float changes and selected history. | Index membership/maintenance scope does not establish full mission company coverage or latest actual company float; not a cohort substitute. |

## Purchase and access capability

The Massive data plugin exposes endpoint search, data calls, SQL over scratch results and scratch workspace management. It has no account subscription, billing or checkout operation. Its exact public self-service route is `https://massive.com/dashboard/subscriptions?checkoutCycle=monthly&checkoutProducts=benzinga_earnings`; the public lookup redirects to signup. The controller subsequently inspected the exact checkout route in the available browser: it redirected to Create Account/signup, confirming no authenticated billing session there. The missing capability is an authenticated, billing-enabled Massive account session or equivalent subscription operation. Necessary payment is already authorized; no further monetary approval is requested.

FactSet's AWS marketplace listing offers **View purchase options** at AWS Data Exchange. It lists a no-cost Product Access dimension but explicitly requires a FactSet ID for the underlying data. This does not prove an unrestricted paid-data license is free or that this mission already has entitlement. No AWS Data Exchange/FactSet account-management connector is exposed here. LSEG/EDI public pages provide sales/contact/login routes, with no public quote or direct machine-readable purchase capability observed. No message has been sent to a third party.

## Qualification and next execution

Raw source completeness, publication-supported historical reconstruction and strict original historical receipt qualification remain independent. Current October downloads can supply exact retained source bytes and dated facts; they cannot create an original August–September receipt. Missing consensus coverage is not zero consensus, and no-coverage events cannot be invented.

Continue original SEC acquisitions and corrected F02/F03/F04 processing while purchasing/access verification remains open. The reviewed `OFFICIAL_SEC_V2` route has one exact-index-pinned document per attempt, 8 MiB/45-second limits, current-owner/stop fencing and a durable normalized successor. The first fresh transport admission is the NNDM 7 August extracted XBRL, while local original documents are reusable without refetching. Company fact packets are facts only; Prompt02 retains all membership/rank decisions.

## Official source links observed

- https://massive.com/docs/rest/partners/benzinga/earnings
- https://massive.com/partners/benzinga
- https://massive.com/partners/business-benzinga
- https://massive.com/knowledge-base/article/does-massive-have-historical-float-data
- https://insight.factset.com/resources/at-a-glance-factset-estimates-point-in-time-consensus
- https://aws.amazon.com/marketplace/pp/prodview-l2x7j552mhvck
- https://www.lseg.com/en/data-catalogue/company-data/ibes-estimates/broker-estimates
- https://www.exchange-data.com/product/free-float-service-edi/
- https://insight.factset.com/resources/at-a-glance-factset-ownership-standard-datafeed
- https://site.financialmodelingprep.com/developer/docs/stable/all-shares-float
- https://sec-api.io/docs/outstanding-shares-float-api
- https://www.lseg.com/en/ftse-russell/index-resources/indices-subscribe-data
