"""Agent scope policy: what the analytics agent may and may not do.

Every refusal carries a machine-readable reason code so the eval suite and the
audit table can be scored without string matching:

===============  ==============================================================
``OUT_OF_SCOPE``      the question is not about this lakehouse's data
``PII_BLOCKED``       the question asks for a classified PII attribute
``UNGRANTED_OBJECT``  the request refers to an object the role cannot read
``UNSAFE_SQL``        the SQL is not a single read-only SELECT, or is malformed
``AMBIGUOUS``         the question cannot be resolved to a concrete metric
===============  ==============================================================

The policy is deliberately *question-level* as well as *SQL-level*: asking for
"player emails" is refused even though the role's SQL would fail later anyway,
because a refusal with a reason is a better answer than an empty result set.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

__all__ = [
    "REASON_CODES",
    "OUT_OF_SCOPE",
    "PII_BLOCKED",
    "UNGRANTED_OBJECT",
    "UNSAFE_SQL",
    "AMBIGUOUS",
    "PolicyDecision",
    "classify_question",
    "PII_TERMS",
    "IN_SCOPE_TERMS",
]

OUT_OF_SCOPE = "OUT_OF_SCOPE"
PII_BLOCKED = "PII_BLOCKED"
UNGRANTED_OBJECT = "UNGRANTED_OBJECT"
UNSAFE_SQL = "UNSAFE_SQL"
AMBIGUOUS = "AMBIGUOUS"

REASON_CODES = (OUT_OF_SCOPE, PII_BLOCKED, UNGRANTED_OBJECT, UNSAFE_SQL, AMBIGUOUS)

# --------------------------------------------------------------------------
# Lexicons
# --------------------------------------------------------------------------

#: words that mean "give me an identifier or a directly identifying attribute"
PII_TERMS: tuple[str, ...] = (
    "email",
    "e-mail",
    "player_ip",
    "ip address",
    "ipaddress",
    "birth year",
    "birth_year",
    "birthday",
    "date of birth",
    "dob",
    "phone",
    "mobile number",
    "contact details",
    "real name",
    "full name",
    "home address",
    "postal",
    "payment method",
    "credit card",
    "card number",
    "who is player",
    "identify the player",
    "personal data",
    "pii",
)

#: words that suggest the question has nothing to do with this lakehouse
OUT_OF_SCOPE_TERMS: tuple[str, ...] = (
    "weather",
    "forecast",
    "stock price",
    "share price",
    "bitcoin",
    "crypto price",
    "recipe",
    "capital of",
    "translate",
    "write a poem",
    "tell me a joke",
    "python script to",
    "javascript",
    "write code",
    "html page",
    "wordpress",
    "flight",
    "hotel",
    "who won the",
    "election",
    "medical advice",
)

#: words that put a question in scope
IN_SCOPE_TERMS: tuple[str, ...] = (
    "dau",
    "daily active",
    "active user",
    "revenue",
    "arpdau",
    "arpu",
    "conversion",
    "monetization",
    "monetisation",
    "payer",
    "purchase",
    "spend",
    "retention",
    "cohort",
    "churn",
    "session",
    "playtime",
    "engagement",
    "match",
    "player",
    "kill",
    "death",
    "table",
    "schema",
    "column",
    "fresh",
    "freshness",
    "stale",
    "row count",
    "how many",
    "trend",
    "top",
    "average",
    "daily",
    "describe",
    "list",
    "fields",
    "available",
)

#: patterns that mean the question is under-specified
AMBIGUOUS_PATTERNS: tuple[str, ...] = (
    r"^show me (the )?(data|numbers|stuff)\.?$",
    r"^how are we doing\??$",
    r"^how('?s| is) it going\??$",
    r"^tell me something\.?$",
    r"^what do you think\??$",
    r"^give me (the )?(data|numbers)\.?$",
    r"^any insights\??$",
    r"^what about (it|that|this)\??$",
    r"^look into it\.?$",
)

_WORD = re.compile(r"[a-z0-9_]+")
_QUALIFIED = re.compile(r"\b(?:bronze|silver|gold|ops|secure)\.[a-z_][a-z0-9_]*", re.IGNORECASE)


@dataclass
class PolicyDecision:
    allowed: bool
    reason_code: str | None = None
    reason: str = ""
    matched: list[str] = field(default_factory=list)

    @property
    def refused(self) -> bool:
        return not self.allowed

    def as_dict(self) -> dict:
        return asdict(self)


def _contains(question: str, terms: tuple[str, ...]) -> list[str]:
    """Match lexicon terms, tolerating simple plural/inflected endings.

    "show me player emails" must hit the ``email`` term, and "addresses" must hit
    ``address``, so single-word terms allow a short lowercase suffix while
    multi-word phrases are matched literally.
    """
    lowered = question.lower()
    found: list[str] = []
    for term in terms:
        if " " in term:
            # multi-word phrase: allow an inflection on the final word only,
            # so "ip addresses" matches the "ip address" term
            head, _, tail = term.rpartition(" ")
            pattern = (
                rf"(?<![a-z0-9_]){re.escape(head)}\s+{re.escape(tail)}[a-z]{{0,4}}(?![a-z0-9_])"
            )
        else:
            pattern = rf"(?<![a-z0-9_]){re.escape(term)}[a-z]{{0,4}}(?![a-z0-9_])"
        if re.search(pattern, lowered):
            found.append(term)
    return found


def classify_question(question: str) -> PolicyDecision:
    """Decide whether a natural-language question may be answered at all."""
    text = (question or "").strip()
    if not text:
        return PolicyDecision(False, AMBIGUOUS, "the question is empty", [])

    pii = _contains(text, PII_TERMS)
    if pii:
        return PolicyDecision(
            False,
            PII_BLOCKED,
            (
                "this asks for a PII-classified attribute ("
                + ", ".join(pii)
                + "). Those columns are masked in the governed views and cannot be "
                "returned through this service."
            ),
            pii,
        )

    out_of_scope = _contains(text, OUT_OF_SCOPE_TERMS)
    if out_of_scope:
        return PolicyDecision(
            False,
            OUT_OF_SCOPE,
            (
                "this is outside the analytics scope of this lakehouse ("
                + ", ".join(out_of_scope)
                + "). I can answer questions about gameplay, monetization, retention "
                "and data freshness."
            ),
            out_of_scope,
        )

    lowered = text.lower()
    for pattern in AMBIGUOUS_PATTERNS:
        if re.match(pattern, lowered):
            return PolicyDecision(
                False,
                AMBIGUOUS,
                (
                    "I could not tell which table or metric you mean. Try naming one, "
                    "for example 'what was the average DAU last week?' or "
                    "'show D7 retention by cohort'."
                ),
                [pattern],
            )

    # A qualified object reference (`secure.x`, `gold.y`) is always in scope.
    qualified = _QUALIFIED.search(text)

    in_scope = _contains(text, IN_SCOPE_TERMS)
    if not in_scope and not qualified and len(_WORD.findall(lowered)) <= 4:
        return PolicyDecision(
            False,
            AMBIGUOUS,
            (
                "I could not tell which table or metric you mean. Try naming one, "
                "for example 'what was the average DAU last week?'"
            ),
            [],
        )

    return PolicyDecision(True, None, "in scope")
