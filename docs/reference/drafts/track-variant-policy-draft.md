# Track Variant Policy Draft

This draft now uses a numeric confidence model instead of a purely binary
"same composition vs separate" policy.

The source-of-truth machine-readable config lives in:

- [track-variant-policy.json](../../config/track-variant-policy.json)

Interpretation:

- `same_composition_default = true` means the family is eligible for default same-composition grouping.
- `base_confidence` is a normalized starting score from `0.0` to `1.0`.
- `separate_default = true` means the family should stay separate unless stronger evidence overrides it later.
- `needs_review = true` means the family is too ambiguous to treat as settled policy.

Reference points:

- `1.0` = essentially the same recording/song-family for analysis
- `0.8` = same song-family but meaningfully different listening object
- lower values = more distant variants
- `0.0` = should not default into the same song-family

| family | same_composition_default | base_confidence | separate_default | needs_review | notes |
| --- | --- | --- | --- | --- | --- |
| remaster | true | 1.00 | false | false | Usually the same composition and same core recording lineage with different mastering. |
| live | true | 0.80 | false | true | Same composition, but often meaningfully different performance context for analysis. |
| acoustic | true | 0.80 | false | true | Often the same composition, but arrangement may matter for some analyses. |
| demo | true | 0.75 | false | true | Usually the same composition, but rough/demo-state recordings may deserve separate treatment in some views. |
| instrumental | true | 0.80 | false | true | Same composition, but missing vocals can materially change genre/taste analysis. |
| version | false | 0.55 | false | true | More ambiguous than previously labeled. Generic labels like `single version` may be same-composition, but attributed versions can behave more like remixes or alternate arrangements. |
| edit | false | 0.55 | false | true | Generic edits may be same-composition; attributed edits may behave like remixes. Needs more signals. |
| remix | false | 0.00 | true | false | Usually a distinct derived version and should stay separate by default. |
| rework | false | 0.00 | true | false | Same reasoning as remix. |
| cover | false | 0.00 | true | false | Separate recording and usually separate artist identity, but should later connect via relationship edges. |
| mix | false | 0.50 | false | true | `original mix` may be same-composition; other mix labels can imply materially different versions. |
| session | true | 0.78 | false | true | Usually the same composition in a special recording context; may be sibling-level rather than canonical-equal. |
| recording_context | true | 0.82 | false | true | Labels like `Recorded at Spotify Studios` usually look same-composition, but context may matter analytically. |
| featured_credit | true | 0.88 | false | true | Same composition if the suffix is just a credit annotation; extra credited artists may also signal a derived version. |
| packaging | true | 0.95 | false | true | `bonus track` and similar packaging labels usually should not split the composition. |
| content_rating | true | 0.97 | false | false | `clean` / `explicit` are usually packaging/content variants of the same composition. |
| format | true | 0.85 | false | true | `mono` / `stereo` often belong together compositionally, but listening-character differences may matter downstream. |
| score_soundtrack | false | 0.55 | false | true | Needs subtype handling: simple `from "X"` placement labels are often groupable, while cue/score-context labels stay more ambiguous. |
| structural | false | 0.00 | true | false | `intro`, `interlude`, `pt. 1`, `skit` usually indicate distinct track forms rather than simple variants. |
| featured_credit_inline | true | 0.88 | false | true | Not currently extracted as a suffix family; inline `(feat. ...)` / `with ...` in the main title may need separate handling. |
| bonus | true | 0.95 | false | true | Legacy exact-label term; folds into `packaging`. |
| commentary | false | 0.00 | true | false | Spoken/commentary tracks should stay separate. |
| wellness_frequency | false | 0.00 | true | false | Frequency/sleep/meditation variants behave like distinct listening objects. |
| year_tag | true | 0.68 | false | true | True 4-digit years are often contextual dating metadata and usually safe to group conservatively, but should stay review-weighted. |
| performance_style | true | 0.72 | false | true | `piano solo`, `orchestral`, `duet` often imply same composition but materially different arrangement. |
| other | false | 0.25 | false | true | Long-tail bucket; do not auto-group by family alone. |

## Follow-Up

- Keep release-track dedupe rules unchanged.
- Do not broaden auto-grouping from this draft alone.
- Add a second-stage classifier for ambiguous families using Spotify contributor metadata.
- Emit a post-pass ambiguous review queue so `needs_review` is a workflow, not just a label.
- Distinguish likely sibling tracks from cousin tracks:
  - sibling: same composition, close recording/arrangement lineage
  - cousin: same composition, but derived/adapted enough that some analyses should keep them apart
- Use contributor deltas as a hint:
  - extra arranger/remixer/editor-style credits can push `edit` / `mix` / `version` toward separate
  - unchanged core artist lineup can push generic packaging/context labels toward same-composition

## Semantic Layer Draft

This is a separate layer above the observed suffix families. The idea is:

- observed family = what the title literally says
- semantic category = what kind of difference it probably represents

We should keep both layers.

### Review Families

These are the families currently marked `needs_review = true`, with a proposed
semantic layer for each.

| observed family | example labels | proposed semantic category | notes |
| --- | --- | --- | --- |
| `live` | `live`, `live 1977`, `live at the bbc, london / 1975` | `performance_context` | Usually same composition, but clearly a different performance event. |
| `acoustic` | `acoustic`, `stripped`, `acoustic demo version` | `arrangement_change` | Same composition, usually different instrumentation or production. |
| `demo` | `demo`, `home demo`, `acoustic demo version` | `recording_state` | Same composition, but pre-release or rough-state recording lineage. |
| `instrumental` | `instrumental`, `instrumental version`, `instrumental album version` | `arrangement_or_vocal_subtraction` | Same composition, but vocals removed or arrangement altered. |
| `version` | `single version`, `original version`, `Natureboy Flako Version` | `version_label_umbrella` | Too broad as one family; should split into subtypes below. |
| `edit` | `edit`, `single edit`, `radio edit`, `Local Natives Edit` | `edit_label_umbrella` | Mix of packaging-like cuts and remix-like attributed transformations. |
| `mix` | `original mix`, `radio mix`, `ambient mix`, `Spike Stent Mix` | `mix_label_umbrella` | Some are harmless packaging, some are distinct producer treatments. |
| `session` | `mahogany sessions`, `john peel session`, `spotify session 2015` | `special_recording_context` | Often same composition, but tied to a named session/venue/series context. |
| `recording_context` | `recorded at spotify studios nyc`, `live from spotify nyc`, `studio` | `capture_context` | Same composition candidate, but not necessarily same recording lineage. |
| `featured_credit` | `feat. guest`, `featuring pharoe monch`, `with ...` | `credit_annotation_or_collab_delta` | Can be harmless credit annotation or signal a materially different derived release. |
| `packaging` | `bonus track`, `bonus`, `expanded` | `release_packaging` | Usually should not split composition, but still worth review until proven consistent. |
| `format` | `mono version`, `stereo version`, `mono` | `format_or_presentation_change` | Usually same composition, but different playback character. |
| `score_soundtrack` | `from "..."`, `soundtrack version`, `score` | `placement_or_context_label` | Sometimes just placement metadata, sometimes a real alternate recording. |
| `featured_credit_inline` | inline `(feat. ...)`, `with ...` in title body | `inline_credit_annotation` | Similar to `featured_credit`, but not currently extracted as a suffix family. |
| `bonus` | `bonus`, `bonus track` | `release_packaging` | Legacy exact-label family that should likely merge semantically with `packaging`. |
| `year_tag` | `2020`, `1975 version`, year-only tags | `dated_revision_or_reissue_marker` | Highly ambiguous without other context. |
| `performance_style` | `solo version`, `orchestral version`, `duet` | `arrangement_change` | Same composition candidate, but clearly different arrangement/performance style. |
| `other` | long-tail misc labels | `unclassified_long_tail` | Preserve exact label first; do not infer too much from the bucket alone. |

### Subtype Splits For Overloaded Families

#### `version`

| observed subfamily | example labels | proposed semantic category | notes |
| --- | --- | --- | --- |
| `generic_version` | `single version`, `album version`, `short version`, `full version` | `packaging_version` | Usually editorial or release-format labeling rather than a deeply different creative object. |
| `generic_version` | `clean version`, `explicit version`, `mono version`, `stereo version` | `content_or_format_version` | Same composition, but altered presentation or format. |
| `generic_version` | `instrumental version` | `arrangement_version` | Same composition, but arrangement/presentation is meaningfully different. |
| `generic_version` | `original version` | `generic_originality_label` | Usually points back to a base version, but still vague without more context. |
| `alternate_version` | `alternate version` | `alternate_take_or_arrangement` | Broad "different from main issue" bucket; can mean alternate take, alternate mix, or alternate arrangement. |
| `performance_style_version` | `solo version`, `orchestral version` | `arrangement_version` | Same composition, clearly different arrangement/performance style. |
| `rerecorded_version` | `re-recorded version`, `rerecorded 2014 version` | `recording_lineage_change` | Same composition, but not the same recording lineage. |
| `year_version` | `1975 version`, `2000 version` | `dated_revision` | Year-labeled revisions are ambiguous and should usually stay review-heavy. |
| `remaster_version` | `remastered version`, `2006 remastered version` | `mastering_or_reissue_label` | Often belongs near `remaster`, but appears through the overloaded `version` wording. |
| `attributed_or_named_version` | `Natureboy Flako Version`, `Djungelns Lag Version`, `Lamentations Version` | `attributed_derived_version` | Often closer to remix/edit logic than to harmless packaging. |
| `other_version` | `version 2 of 2`, `soundtrack version`, `studio version duet` | `ambiguous_version_misc` | Catch-all for remaining `version` phrasing that needs further splitting. |

#### `edit`

| observed subfamily | example labels | proposed semantic category | notes |
| --- | --- | --- | --- |
| `generic_edit` | `edit`, `single edit`, `video edit` | `packaging_edit` | Often shortened or reissued for format/radio purposes. |
| `radio_edit` | `radio edit`, `tribal radio edit` | `broadcast_length_or_content_edit` | Usually same composition, but not always trivial. |
| `attributed_edit` | `Local Natives Edit`, `Arnold Palmer Edit` | `attributed_derived_version` | Often behaves much closer to remix logic. |
| `compound_edit` | `glassforms version edit`, `studio edit` | `ambiguous_edit_misc` | Mixed phrasing that likely needs artist-credit/context signals. |

#### `mix`

| observed subfamily | example labels | proposed semantic category | notes |
| --- | --- | --- | --- |
| `original_mix` | `original mix` | `base_release_mix_label` | Often same composition and near-base issue within dance/electronic catalogs. |
| `generic_mix` | `radio mix`, `extended mix`, `ambient mix` | `mix_treatment` | Usually same composition, but meaningfully different listening object. |
| `attributed_mix` | `Spike Stent Mix`, `Iggy Mix` | `attributed_derived_version` | Strong producer/mixer identity; often remix-adjacent. |
| `misc_mix` | `mixed`, `mixtape`-like suffix cases | `ambiguous_mix_misc` | Needs more careful handling. |

### Current Interpretation Notes

- Not every trailing parenthetical should be treated as variant metadata.
- Internal title hyphens should be preserved unless there is a clear spaced suffix separator like ` - `.
- Stacked labels should be interpreted as components with a dominant lineage.
  Example: `Puente Roto (Quantic Remix) - Radio Edit`
  - dominant lineage: `remix`
  - secondary modifier: `radio edit`
  - implication: group within the remix lineage, not back to the original song

### Review Workflow

The current implementation now emits an ambiguous review log immediately after the analysis grouping refresh. That log should include:

- release track title
- artist
- current analysis assignment, if any
- extracted base title
- dominant family
- all interpreted components

This makes `needs_review = true` actionable instead of passive metadata.

### Notes On `generic_version`

For now, I would treat `generic_version` as an observed family, not as one final semantic bucket.

Examples from the DB:

- `California Love - Original Version`
- `Roll Over Beethoven - Single Version`
- `Maybellene - Single Version`
- `Yinde (Short Version)`
- `Final Eclipse - Full Version`
- `Questions - Instrumental Version`

So the likely semantic split is:

- `packaging_version`
- `content_or_format_version`
- `arrangement_version`
- `generic_originality_label`
