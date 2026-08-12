from __future__ import annotations

# Compatibility entry point for any Blueprint/service configuration that already
# references app.worker_with_xal. The XAL-006 monitor is now started inside the
# canonical app.worker process, so this shim deliberately does not start a
# second monitor thread.
from app.worker import main


if __name__ == "__main__":
    main()
