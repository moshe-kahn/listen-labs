from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from backend.app.config import BACKEND_DIR


POLICY_PATH = BACKEND_DIR.parent / "docs" / "config" / "track-variant-policy.json"


@dataclass(frozen=True)
class TrackVariantSubtypePolicy:
    observed_subfamily: str
    example_labels: tuple[str, ...]
    semantic_category: str
    notes: str


@dataclass(frozen=True)
class TrackVariantFamilyPolicy:
    family: str
    same_composition_default: bool
    base_confidence: float
    separate_default: bool
    needs_review: bool
    semantic_category: str
    notes: str
    example_labels: tuple[str, ...]
    subtypes: tuple[TrackVariantSubtypePolicy, ...]


@dataclass(frozen=True)
class TrackVariantPolicy:
    model_version: int
    description: str
    confidence_scale: dict[str, float]
    families: tuple[TrackVariantFamilyPolicy, ...]

    def get_family(self, family: str) -> TrackVariantFamilyPolicy | None:
        for item in self.families:
            if item.family == family:
                return item
        return None

    @property
    def review_families(self) -> tuple[TrackVariantFamilyPolicy, ...]:
        return tuple(item for item in self.families if item.needs_review)


@dataclass(frozen=True)
class TrackVariantComponent:
    raw_label: str
    normalized_label: str
    family: str
    semantic_category: str
    groupable_by_default: bool


@dataclass(frozen=True)
class TrackVariantInterpretation:
    base_title_anchor: str
    dominant_family: str | None
    dominant_semantic_category: str | None
    components: tuple[TrackVariantComponent, ...]


GROUPABLE_VERSION_SEMANTIC_CATEGORIES = frozenset(
    {
        "packaging_version",
        "content_or_format_version",
        "arrangement_version",
        "generic_originality_label",
        "alternate_take_or_arrangement",
    }
)
GROUPABLE_EDIT_SEMANTIC_CATEGORIES = frozenset(
    {
        "packaging_edit",
        "broadcast_length_or_content_edit",
    }
)
GROUPABLE_MIX_SEMANTIC_CATEGORIES = frozenset(
    {
        "base_release_mix_label",
        "broadcast_mix_treatment",
        "format_or_presentation_change",
        "mix_treatment",
    }
)
GROUPABLE_SCORE_SOUNDTRACK_SEMANTIC_CATEGORIES = frozenset(
    {
        "placement_or_context_label",
    }
)
TRAILING_BRACKET_BLOCK_PATTERN = re.compile(r"\s*[\(\[]([^\)\]]+)[\)\]]\s*$")
TRAILING_DASH_BLOCK_PATTERN = re.compile(r"\s(?:-|–|—|:)\s([^-–—:\(\)\[\]]+)\s*$")
DOMINANT_FAMILY_PRIORITY = {
    "cover": 100,
    "remix": 95,
    "rework": 94,
    "mix": 70,
    "edit": 65,
    "version": 60,
    "live": 55,
    "acoustic": 54,
    "instrumental": 53,
    "demo": 52,
    "remaster": 40,
    "session": 35,
    "recording_context": 34,
    "featured_credit": 20,
    "packaging": 15,
    "content_rating": 10,
    "format": 10,
}
YEAR_TAG_PATTERN = re.compile(r"[12]\d{3}")
REVIEW_WELLNESS_PATTERN = re.compile(r"\b(?:sleep|meditation|alpha waves|delta waves|theta waves|\d{2,4}\s*hz)\b")


def _normalize_variant_label(value: str) -> str:
    return " ".join(str(value).strip().lower().split())


def _family_policy_component(
    family_name: str,
    label: str,
    normalized: str,
    *,
    semantic_override: str | None = None,
    groupable_override: bool | None = None,
) -> TrackVariantComponent | None:
    family = load_track_variant_policy().get_family(family_name)
    if family is None:
        return None
    return TrackVariantComponent(
        raw_label=label,
        normalized_label=normalized,
        family=family_name,
        semantic_category=family.semantic_category if semantic_override is None else semantic_override,
        groupable_by_default=family.same_composition_default if groupable_override is None else groupable_override,
    )


def _extract_trailing_variant_label(value: str) -> tuple[str, str, str] | None:
    working = str(value).strip()
    bracket_match = TRAILING_BRACKET_BLOCK_PATTERN.search(working)
    if bracket_match:
        return (
            working[: bracket_match.start()].strip(),
            bracket_match.group(1).strip(),
            "bracket",
        )
    dash_match = TRAILING_DASH_BLOCK_PATTERN.search(working)
    if dash_match:
        return (
            working[: dash_match.start()].strip(),
            dash_match.group(1).strip(),
            "dash",
        )
    return None


def _load_policy_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _coerce_subtype(data: dict[str, object]) -> TrackVariantSubtypePolicy:
    return TrackVariantSubtypePolicy(
        observed_subfamily=str(data["observed_subfamily"]),
        example_labels=tuple(str(label) for label in data.get("example_labels", [])),
        semantic_category=str(data["semantic_category"]),
        notes=str(data["notes"]),
    )


def _coerce_family(data: dict[str, object]) -> TrackVariantFamilyPolicy:
    return TrackVariantFamilyPolicy(
        family=str(data["family"]),
        same_composition_default=bool(data["same_composition_default"]),
        base_confidence=float(data["base_confidence"]),
        separate_default=bool(data["separate_default"]),
        needs_review=bool(data["needs_review"]),
        semantic_category=str(data["semantic_category"]),
        notes=str(data["notes"]),
        example_labels=tuple(str(label) for label in data.get("example_labels", [])),
        subtypes=tuple(_coerce_subtype(dict(item)) for item in data.get("subtypes", [])),
    )


@lru_cache(maxsize=1)
def load_track_variant_policy() -> TrackVariantPolicy:
    raw = _load_policy_json(POLICY_PATH)
    return TrackVariantPolicy(
        model_version=int(raw["model_version"]),
        description=str(raw["description"]),
        confidence_scale={str(key): float(value) for key, value in dict(raw["confidence_scale"]).items()},
        families=tuple(_coerce_family(dict(item)) for item in raw["families"]),
    )


@lru_cache(maxsize=1)
def _version_subtype_label_map() -> dict[str, TrackVariantSubtypePolicy]:
    policy = load_track_variant_policy()
    version_family = policy.get_family("version")
    mapping: dict[str, TrackVariantSubtypePolicy] = {}
    if version_family is None:
        return mapping
    for subtype in version_family.subtypes:
        for label in subtype.example_labels:
            mapping[_normalize_variant_label(label)] = subtype
    return mapping


@lru_cache(maxsize=1)
def _edit_subtype_label_map() -> dict[str, TrackVariantSubtypePolicy]:
    policy = load_track_variant_policy()
    edit_family = policy.get_family("edit")
    mapping: dict[str, TrackVariantSubtypePolicy] = {}
    if edit_family is None:
        return mapping
    for subtype in edit_family.subtypes:
        for label in subtype.example_labels:
            mapping[_normalize_variant_label(label)] = subtype
    return mapping


@lru_cache(maxsize=1)
def _mix_subtype_label_map() -> dict[str, TrackVariantSubtypePolicy]:
    policy = load_track_variant_policy()
    mix_family = policy.get_family("mix")
    mapping: dict[str, TrackVariantSubtypePolicy] = {}
    if mix_family is None:
        return mapping
    for subtype in mix_family.subtypes:
        for label in subtype.example_labels:
            mapping[_normalize_variant_label(label)] = subtype
    return mapping


@lru_cache(maxsize=1)
def _score_soundtrack_subtype_label_map() -> dict[str, TrackVariantSubtypePolicy]:
    policy = load_track_variant_policy()
    family = policy.get_family("score_soundtrack")
    mapping: dict[str, TrackVariantSubtypePolicy] = {}
    if family is None:
        return mapping
    for subtype in family.subtypes:
        for label in subtype.example_labels:
            mapping[_normalize_variant_label(label)] = subtype
    return mapping


def classify_version_label(label: str | None) -> TrackVariantSubtypePolicy | None:
    if label is None:
        return None
    normalized = _normalize_variant_label(label)
    if not normalized or "version" not in normalized:
        return None

    exact_match = _version_subtype_label_map().get(normalized)
    if exact_match is not None:
        return exact_match

    version_family = load_track_variant_policy().get_family("version")
    if version_family is None:
        return None

    subtype_by_name = {item.observed_subfamily: item for item in version_family.subtypes}
    if "remaster" in normalized or "remastered" in normalized:
        return subtype_by_name.get("remaster_version")
    if "re-record" in normalized or "rerecord" in normalized:
        return subtype_by_name.get("rerecorded_version")
    if re.fullmatch(r"\d{4} version", normalized):
        return subtype_by_name.get("year_version")
    if normalized.endswith("version"):
        return subtype_by_name.get("attributed_or_named_version") or subtype_by_name.get("other_version")
    return subtype_by_name.get("other_version")


def is_groupable_version_label(label: str | None) -> bool:
    subtype = classify_version_label(label)
    if subtype is None:
        return False
    return subtype.semantic_category in GROUPABLE_VERSION_SEMANTIC_CATEGORIES


def classify_edit_label(label: str | None) -> TrackVariantSubtypePolicy | None:
    if label is None:
        return None
    normalized = _normalize_variant_label(label)
    if not normalized or "edit" not in normalized:
        return None

    exact_match = _edit_subtype_label_map().get(normalized)
    if exact_match is not None:
        return exact_match

    edit_family = load_track_variant_policy().get_family("edit")
    if edit_family is None:
        return None

    subtype_by_name = {item.observed_subfamily: item for item in edit_family.subtypes}
    if "radio edit" in normalized:
        return subtype_by_name.get("radio_edit")
    if normalized.endswith("edit"):
        return subtype_by_name.get("attributed_edit") or subtype_by_name.get("compound_edit")
    return subtype_by_name.get("compound_edit")


def is_groupable_edit_label(label: str | None) -> bool:
    subtype = classify_edit_label(label)
    if subtype is None:
        return False
    return subtype.semantic_category in GROUPABLE_EDIT_SEMANTIC_CATEGORIES


def classify_mix_label(label: str | None) -> TrackVariantSubtypePolicy | None:
    if label is None:
        return None
    normalized = _normalize_variant_label(label)
    if not normalized or "mix" not in normalized:
        return None

    exact_match = _mix_subtype_label_map().get(normalized)
    if exact_match is not None:
        return exact_match

    mix_family = load_track_variant_policy().get_family("mix")
    if mix_family is None:
        return None

    subtype_by_name = {item.observed_subfamily: item for item in mix_family.subtypes}
    if normalized == "original mix":
        return subtype_by_name.get("original_mix")
    if normalized.endswith("stereo mix") or normalized.endswith("mono mix"):
        return subtype_by_name.get("presentation_mix") or subtype_by_name.get("generic_mix")
    if normalized.endswith("mix"):
        return subtype_by_name.get("attributed_mix") or subtype_by_name.get("generic_mix")
    if "mixed" in normalized or "mixtape" in normalized:
        return subtype_by_name.get("misc_mix")
    return subtype_by_name.get("misc_mix")


def is_groupable_mix_label(label: str | None) -> bool:
    subtype = classify_mix_label(label)
    if subtype is None:
        return False
    return subtype.semantic_category in GROUPABLE_MIX_SEMANTIC_CATEGORIES


def classify_score_soundtrack_label(label: str | None) -> TrackVariantSubtypePolicy | None:
    if label is None:
        return None
    normalized = _normalize_variant_label(label)
    if not normalized:
        return None

    exact_match = _score_soundtrack_subtype_label_map().get(normalized)
    if exact_match is not None:
        return exact_match

    family = load_track_variant_policy().get_family("score_soundtrack")
    if family is None:
        return None

    subtype_by_name = {item.observed_subfamily: item for item in family.subtypes}
    if normalized.startswith('from "') or normalized.startswith("from the original motion picture soundtrack"):
        if "score" in normalized:
            return subtype_by_name.get("score_cue_context")
        return subtype_by_name.get("placement_context_label")
    if "soundtrack" in normalized or "score" in normalized:
        return subtype_by_name.get("score_cue_context")
    return None


def is_groupable_score_soundtrack_label(label: str | None) -> bool:
    subtype = classify_score_soundtrack_label(label)
    if subtype is None:
        return False
    return subtype.semantic_category in GROUPABLE_SCORE_SOUNDTRACK_SEMANTIC_CATEGORIES


def classify_label_families(label: str | None) -> tuple[TrackVariantComponent, ...]:
    if label is None:
        return ()
    normalized = _normalize_variant_label(label)
    if not normalized:
        return ()

    components: list[TrackVariantComponent] = []

    if "cover" in normalized:
        component = _family_policy_component("cover", label, normalized)
        if component is not None:
            components.append(component)
    if "remix" in normalized:
        component = _family_policy_component("remix", label, normalized)
        if component is not None:
            components.append(component)
    if "rework" in normalized:
        component = _family_policy_component("rework", label, normalized)
        if component is not None:
            components.append(component)

    edit_subtype = classify_edit_label(normalized)
    if edit_subtype is not None:
        components.append(
            TrackVariantComponent(
                raw_label=label,
                normalized_label=normalized,
                family="edit",
                semantic_category=edit_subtype.semantic_category,
                groupable_by_default=edit_subtype.semantic_category in GROUPABLE_EDIT_SEMANTIC_CATEGORIES,
            )
        )

    version_subtype = classify_version_label(normalized)
    if version_subtype is not None:
        components.append(
            TrackVariantComponent(
                raw_label=label,
                normalized_label=normalized,
                family="version",
                semantic_category=version_subtype.semantic_category,
                groupable_by_default=version_subtype.semantic_category in GROUPABLE_VERSION_SEMANTIC_CATEGORIES,
            )
        )

    mix_subtype = classify_mix_label(normalized)
    if mix_subtype is not None:
        components.append(
            TrackVariantComponent(
                raw_label=label,
                normalized_label=normalized,
                family="mix",
                semantic_category=mix_subtype.semantic_category,
                groupable_by_default=mix_subtype.semantic_category in GROUPABLE_MIX_SEMANTIC_CATEGORIES,
            )
        )

    if "live" in normalized:
        component = _family_policy_component("live", label, normalized)
        if component is not None:
            components.append(component)
    if "acoustic" in normalized or "stripped" in normalized:
        component = _family_policy_component("acoustic", label, normalized)
        if component is not None:
            components.append(component)
    if "demo" in normalized:
        component = _family_policy_component("demo", label, normalized)
        if component is not None:
            components.append(component)
    if "instrumental" in normalized:
        component = _family_policy_component("instrumental", label, normalized)
        if component is not None:
            components.append(component)
    if "remaster" in normalized or "remastered" in normalized:
        component = _family_policy_component("remaster", label, normalized)
        if component is not None:
            components.append(component)
    if "explicit" in normalized or "clean" in normalized:
        component = _family_policy_component("content_rating", label, normalized)
        if component is not None:
            components.append(component)
    if "mono" in normalized or "stereo" in normalized:
        component = _family_policy_component("format", label, normalized)
        if component is not None:
            components.append(component)
    if normalized in {"bonus", "bonus track"} or "expanded" in normalized or "deluxe" in normalized:
        component = _family_policy_component("packaging", label, normalized)
        if component is not None:
            components.append(component)
    if "session" in normalized or "sessions" in normalized:
        component = _family_policy_component("session", label, normalized)
        if component is not None:
            components.append(component)
    if normalized.startswith("recorded at ") or normalized.startswith("recorded live at ") or normalized.startswith("live from ") or "spotify studios" in normalized or normalized == "studio":
        component = _family_policy_component("recording_context", label, normalized)
        if component is not None:
            components.append(component)
    if normalized.startswith("feat.") or normalized.startswith("featuring ") or normalized.startswith("with "):
        component = _family_policy_component("featured_credit", label, normalized)
        if component is not None:
            components.append(component)
    soundtrack_subtype = classify_score_soundtrack_label(normalized)
    if soundtrack_subtype is not None:
        components.append(
            TrackVariantComponent(
                raw_label=label,
                normalized_label=normalized,
                family="score_soundtrack",
                semantic_category=soundtrack_subtype.semantic_category,
                groupable_by_default=soundtrack_subtype.semantic_category in GROUPABLE_SCORE_SOUNDTRACK_SEMANTIC_CATEGORIES,
            )
        )
    if normalized == "intro" or normalized == "outro" or "interlude" in normalized or "skit" in normalized or "reprise" in normalized or normalized.startswith("pt. ") or normalized.startswith("part "):
        component = _family_policy_component("structural", label, normalized)
        if component is not None:
            components.append(component)
    if "commentary" in normalized:
        component = _family_policy_component("commentary", label, normalized)
        if component is not None:
            components.append(component)
    if REVIEW_WELLNESS_PATTERN.search(normalized):
        component = _family_policy_component("wellness_frequency", label, normalized)
        if component is not None:
            components.append(component)
    if YEAR_TAG_PATTERN.fullmatch(normalized):
        component = _family_policy_component("year_tag", label, normalized)
        if component is not None:
            components.append(component)
    if "solo" in normalized or "duet" in normalized or "trio" in normalized or "quartet" in normalized or "orchestral" in normalized:
        component = _family_policy_component("performance_style", label, normalized)
        if component is not None:
            components.append(component)

    deduped: list[TrackVariantComponent] = []
    seen_keys: set[tuple[str, str]] = set()
    for component in components:
        key = (component.family, component.semantic_category)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(component)
    return tuple(deduped)


def classify_variant_component(label: str | None) -> TrackVariantComponent | None:
    components = classify_label_families(label)
    return None if not components else components[0]


def interpret_track_variant_title(title: str | None) -> TrackVariantInterpretation:
    working = "" if title is None else str(title).strip()
    components: list[TrackVariantComponent] = []

    while working:
        extracted = _extract_trailing_variant_label(working)
        if extracted is None:
            break
        remaining, raw_label, _kind = extracted
        component = classify_variant_component(raw_label)
        if component is None:
            break
        components.append(component)
        working = remaining

    dominant: TrackVariantComponent | None = None
    dominant_priority = -1
    for component in components:
        priority = DOMINANT_FAMILY_PRIORITY.get(component.family, 0)
        if priority > dominant_priority:
            dominant = component
            dominant_priority = priority

    return TrackVariantInterpretation(
        base_title_anchor=working,
        dominant_family=None if dominant is None else dominant.family,
        dominant_semantic_category=None if dominant is None else dominant.semantic_category,
        components=tuple(components),
    )
