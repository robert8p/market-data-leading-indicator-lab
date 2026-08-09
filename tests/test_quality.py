from app.quality import summarize_severities


def test_quality_passes_with_info_only():
    summary = summarize_severities(["info", "info"])
    assert summary["status"] == "pass"
    assert summary["analysis_ready"] is True
    assert summary["errors"] == 0
    assert summary["warnings"] == 0


def test_quality_requires_review_for_warning():
    summary = summarize_severities(["info", "warning"])
    assert summary["status"] == "review"
    assert summary["analysis_ready"] is False
    assert summary["warnings"] == 1


def test_quality_fails_for_error_even_with_warning():
    summary = summarize_severities(["info", "warning", "error"])
    assert summary["status"] == "fail"
    assert summary["analysis_ready"] is False
    assert summary["errors"] == 1
