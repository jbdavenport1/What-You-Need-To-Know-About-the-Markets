import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


@dataclass
class ComplianceIssue:
    field: str
    severity: str
    pattern: str
    original_excerpt: str
    action: str


DEFAULT_DISCLAIMER = (
    "This material is for informational purposes only. It is not personalized investment advice "
    "or a recommendation to buy or sell any security. Investing involves risk, including possible loss of principal."
)


def _clean_whitespace(text: str) -> str:
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


REPLACEMENT_RULES: List[Tuple[re.Pattern[str], str, str]] = [
    (re.compile(r"\bguarantee(?:d|s)?\b", re.IGNORECASE), "no certainty", "Replaced guarantee language"),
    (re.compile(r"\brisk[- ]free\b", re.IGNORECASE), "lower-risk", "Replaced risk-free language"),
    (re.compile(r"\byou should buy\b", re.IGNORECASE), "investors should review any portfolio changes with their advisor", "Removed directive buy language"),
    (re.compile(r"\byou should sell\b", re.IGNORECASE), "investors should review any portfolio changes with their advisor", "Removed directive sell language"),
    (re.compile(r"\bwill rise\b", re.IGNORECASE), "could move higher", "Softened market prediction"),
    (re.compile(r"\bwill fall\b", re.IGNORECASE), "could move lower", "Softened market prediction"),
    (re.compile(r"\bwill increase\b", re.IGNORECASE), "could increase", "Softened market prediction"),
    (re.compile(r"\bwill decrease\b", re.IGNORECASE), "could decrease", "Softened market prediction"),
    (re.compile(r"\boutperform\b", re.IGNORECASE), "perform differently from", "Softened performance claim"),
    (re.compile(r"\bbeat the market\b", re.IGNORECASE), "perform differently from the broader market", "Softened performance claim"),
    (re.compile(r"\bcan(?:not|'t) lose\b", re.IGNORECASE), "still involves risk", "Removed absolute claim"),
    (re.compile(r"\bcertain(?:ly)?\b", re.IGNORECASE), "not certain", "Softened certainty language"),
]

FLAG_ONLY_RULES: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bguaranteed returns?\b", re.IGNORECASE), "High-risk returns claim"),
    (re.compile(r"\bno risk\b", re.IGNORECASE), "Absolute risk claim"),
    (re.compile(r"\brecommend(?:ation)? to buy\b", re.IGNORECASE), "Explicit recommendation language"),
    (re.compile(r"\brecommend(?:ation)? to sell\b", re.IGNORECASE), "Explicit recommendation language"),
]


TEXT_FIELDS = ["client_email_subject", "client_email_body", "newsletter_body", "linkedin_post"]
LIST_FIELDS = ["advisor_talking_points"]


def sanitize_text(field: str, text: str, issues: List[ComplianceIssue]) -> str:
    value = _clean_whitespace(text)
    for pattern, replacement, action in REPLACEMENT_RULES:
        for match in pattern.finditer(value):
            issues.append(
                ComplianceIssue(
                    field=field,
                    severity="medium",
                    pattern=pattern.pattern,
                    original_excerpt=match.group(0),
                    action=action,
                )
            )
        value = pattern.sub(replacement, value)

    for pattern, action in FLAG_ONLY_RULES:
        for match in pattern.finditer(value):
            issues.append(
                ComplianceIssue(
                    field=field,
                    severity="high",
                    pattern=pattern.pattern,
                    original_excerpt=match.group(0),
                    action=action,
                )
            )

    return value.strip()


def apply_compliance_filter(content: Dict[str, Any], disclaimer: str = DEFAULT_DISCLAIMER) -> Dict[str, Any]:
    filtered = dict(content)
    issues: List[ComplianceIssue] = []

    for field in TEXT_FIELDS:
        filtered[field] = sanitize_text(field, str(filtered.get(field, "")), issues)

    talking_points = filtered.get("advisor_talking_points", []) or []
    cleaned_points: List[str] = []
    for idx, point in enumerate(talking_points):
        cleaned_points.append(sanitize_text(f"advisor_talking_points[{idx}]", str(point), issues))
    filtered["advisor_talking_points"] = cleaned_points[:7]

    if not filtered.get("client_email_subject"):
        filtered["client_email_subject"] = "What You Need To Know About the Markets"

    filtered["compliance_disclaimer"] = disclaimer
    filtered["compliance_summary"] = {
        "issue_count": len(issues),
        "high_severity_count": sum(1 for i in issues if i.severity == "high"),
        "issues": [asdict(issue) for issue in issues],
    }
    return filtered


def save_compliance_report(report_path: Path, content: Dict[str, Any]) -> None:
    summary = content.get("compliance_summary", {})
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
