"""Check the published demonstration pages and their evidence bundle."""

import hashlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SHOWCASE = ROOT / "showcase"
sys.path.insert(0, str(SHOWCASE))

from app import app  # noqa: E402


@pytest.mark.parametrize("route", ["/", "/architecture", "/lineage", "/dq"])
def test_demo_pages_identify_scope(route: str) -> None:
    response = app.test_client().get(route)
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'lang="pt-BR"' in html
    assert "Demonstração técnica" in html
    assert "Dados públicos e históricos" in html
    assert "manifesto da demonstração" in html


def test_evidence_images_match_manifest() -> None:
    evidence_dir = SHOWCASE / "static" / "evidence"
    manifest = json.loads((evidence_dir / "manifest.json").read_text(encoding="utf-8"))

    assert manifest["kind"] == "technical_demo_snapshot"
    assert manifest["dbt_tests"]["passed"] == manifest["dbt_tests"]["total"]
    assert manifest["great_expectations"]["violations"] == 0
    for name, expected_hash in manifest["images_sha256"].items():
        assert hashlib.sha256((evidence_dir / name).read_bytes()).hexdigest() == expected_hash
