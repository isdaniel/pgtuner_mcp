"""Unit tests for server-level security behavior."""

import pytest


class TestCorsConfig:
    def test_default_uses_localhost_regex(self, monkeypatch):
        from pgtuner_mcp.server import _resolve_cors_config
        monkeypatch.delenv("PGTUNER_CORS_ALLOW_ORIGINS", raising=False)
        cfg = _resolve_cors_config()
        # Default uses regex (any port) rather than a literal list
        assert cfg["allow_origin_regex"] is not None
        assert "localhost" in cfg["allow_origin_regex"]
        assert "127" in cfg["allow_origin_regex"]
        assert cfg["allow_origins"] == []
        assert cfg["allow_credentials"] is True
        # Sanity: regex actually matches sample localhost origins
        import re
        rx = re.compile(cfg["allow_origin_regex"])
        assert rx.match("http://localhost")
        assert rx.match("http://localhost:8080")
        assert rx.match("http://127.0.0.1:3000")
        assert rx.match("https://localhost:5173")
        assert not rx.match("http://evil.example")

    def test_explicit_list_preserves_credentials(self, monkeypatch):
        from pgtuner_mcp.server import _resolve_cors_config
        monkeypatch.setenv("PGTUNER_CORS_ALLOW_ORIGINS", "https://a.example,https://b.example")
        cfg = _resolve_cors_config()
        assert cfg["allow_origins"] == ["https://a.example", "https://b.example"]
        assert cfg["allow_credentials"] is True

    def test_wildcard_forces_no_credentials(self, monkeypatch):
        from pgtuner_mcp.server import _resolve_cors_config
        monkeypatch.setenv("PGTUNER_CORS_ALLOW_ORIGINS", "*")
        cfg = _resolve_cors_config()
        assert cfg["allow_origins"] == ["*"]
        assert cfg["allow_credentials"] is False

    def test_wildcard_in_mixed_list_forces_no_credentials(self, monkeypatch):
        """Wildcard appearing alongside other origins must still disable
        allow_credentials — Starlette rejects '*' + credentials at startup."""
        from pgtuner_mcp.server import _resolve_cors_config
        monkeypatch.setenv(
            "PGTUNER_CORS_ALLOW_ORIGINS", "https://app.example,*"
        )
        cfg = _resolve_cors_config()
        assert "*" in cfg["allow_origins"]
        assert cfg["allow_credentials"] is False


class TestErrorScrub:
    def test_scrub_uri_in_error_message(self):
        from pgtuner_mcp.server import _scrub_error_text
        msg = "boom postgresql://app:hunter2@db:5432/x boom"
        out = _scrub_error_text(msg)
        assert "hunter2" not in out
        assert "****" in out
