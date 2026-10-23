# Independent check of the retained Massive minute pilot

Run `massive_minute_independent_888_v1.sql` unchanged through the root controller's database connection. It takes no parameters and makes no provider request. The author has not executed it remotely.

The check is restricted to the observed AAPL September 8, 2026 archive ending in `d3ed13031b937c7f84d0017dc5432cdee2b75d73429464e02a7c26c4a4d18b5d`: one terminal page, 888 source occurrences, 90,747 bytes, and source SHA-256 `b227d73395a38f11d5838b98235834aeb9138dc666837a00d0c0adbd121f308a`. It does not widen the admitted source window or derive new P02 output.

The transaction is read-only at READ COMMITTED isolation. A single anonymous block has an eight-second statement timeout and a 500-millisecond lock timeout. It captures fresh source, typed-value and row-version manifests before and after the reconstruction. No table, function, task, receipt or certification is created or changed. A transaction-local setting carries the final JSON result to the final SELECT.

The independent SQL decodes the retained JSON, reconstructs numeric and boolean values, preserves null and missing values, computes the declared missing/invalid/structural masks and hashes every raw record. Timestamp conversion uses integer seconds and milliseconds from the epoch, without the producer's floating-point `to_timestamp` conversion. A full join detects missing and unexpected typed rows, including one extra row beyond the expected 888. The current typed batch hash must match both the independently reconstructed values and the saved proof.

The check also binds the exact source URL, byte/hash evidence, source-page manifest, completed acquisition/normalization/derivation tasks, acquisition attempt and released provider lease, source commit, immutable input versions, provisional partition and frozen September 8 calendar. Observed rows are classified into premarket, regular, postmarket and other New York-day times. Those counts are observed source rows, not a dense-clock completeness denominator. The provisional view is checked for source/timestamp linkage and unqualified company, SIP, publication and historical-receipt flags.

`PASS_SOURCE_TRANSFORMATION_ONLY` requires all 20 mismatch counters to be zero, valid equal independently reconstructed/current typed hashes and identical before/after manifests. Any SQL error, timeout, missing final result, nonzero mismatch or changed manifest prevents a PASS. Root should retain the complete result together with the exact SQL hash.

A successful result does not establish independent provider price accuracy, complete expected printed bars, the 3,000-company denominator, source-feed identity, original wire observation, historical receipt or publication eligibility. No missing bars are created or interpreted as zero volume.
