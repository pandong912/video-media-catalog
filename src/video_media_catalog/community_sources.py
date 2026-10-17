"""Reviewed bootstrap registry for the community-first catalog foundation."""

from __future__ import annotations

from video_media_catalog.constants import (
    DOUBAN_LEGACY_NAMESPACE_ID,
    DOUBAN_LEGACY_SCHEME,
    DOUBAN_PERSON_NAMESPACE_ID,
    DOUBAN_WORK_NAMESPACE_ID,
)
from video_media_catalog.douban import DOUBAN_IDENTIFIER_PATTERN
from video_media_catalog.europeana import (
    europeana_metadata_rights_profile,
    europeana_registry_entries,
)
from video_media_catalog.imdb import (
    IMDB_COMPANY_NAMESPACE_ID,
    IMDB_NAME_NAMESPACE_ID,
    IMDB_TITLE_NAMESPACE_ID,
    imdb_registry_entries,
    imdb_rights_profile,
)
from video_media_catalog.rights import PolicyZone, RightsProfile, UsageAction
from video_media_catalog.source_mappers import WIKIDATA_FULL_MEDIA_CONNECTOR_ID
from video_media_catalog.source_registry import (
    SchemaCompatibility,
    SchemaContract,
    SourceNamespace,
    SourceProduct,
    SourceProductKind,
    SourceRegistrySnapshot,
    SourceSystem,
)
from video_media_catalog.tmdb import (
    tmdb_registry_entries,
    tmdb_rights_profile,
)
from video_media_catalog.tvmaze import (
    tvmaze_registry_entries,
    tvmaze_rights_profile,
)
from video_media_catalog.v2_contracts import digest_identity

EIDR_EXACT_LOOKUP_CONNECTOR_ID = "eidr-discovered-id-exact-lookup"
OPENFOOTBALL_SNAPSHOT_CONNECTOR_ID = "openfootball-git-snapshot"
STATSBOMB_SNAPSHOT_CONNECTOR_ID = "statsbomb-open-data-snapshot"
MADB_SNAPSHOT_CONNECTOR_ID = "madb-public-data-snapshot"
MUSICBRAINZ_CORE_DUMP_CONNECTOR_ID = "musicbrainz-core-dump-snapshot"


def research_source_schema_contracts() -> tuple[SchemaContract, ...]:
    """Schema identities pinned by global-reference capture manifests."""

    definitions = (
        (
            "wikidata-research-entity",
            "application/x-ndjson",
            SchemaCompatibility.ADDITIVE,
            {
                "required": ["id", "claims"],
                "profiles": ["sports", "animation"],
            },
        ),
        (
            "wikidata-global-reference-entity",
            "application/x-ndjson",
            SchemaCompatibility.ADDITIVE,
            {
                "required": ["id", "researchDomain", "entityTypeHint"],
                "profiles": [
                    "news-journalism",
                    "learning-instruction",
                    "music",
                ],
            },
        ),
        (
            "openfootball-event",
            "application/json",
            SchemaCompatibility.VERSIONED,
            {
                "required": ["competition", "season", "events"],
                "snapshot": "git-commit",
            },
        ),
        (
            "statsbomb-open-data",
            "application/json",
            SchemaCompatibility.VERSIONED,
            {
                "required": ["competitionId", "seasonId", "matchId"],
                "snapshot": "git-commit",
            },
        ),
        (
            "madb-animation-record",
            "application/json",
            SchemaCompatibility.ADDITIVE,
            {
                "required": ["id", "title"],
                "profiles": ["animation"],
            },
        ),
        (
            "musicbrainz-core-entity",
            "application/x-ndjson",
            SchemaCompatibility.ADDITIVE,
            {
                "required": ["id", "researchDomain", "entityType", "coreDataset"],
                "profiles": ["music"],
                "coreDataset": "MUSICBRAINZ_CORE_CC0",
                "excluded": [
                    "annotations",
                    "cover-art",
                    "genres",
                    "live-feed",
                    "ratings",
                    "supplementary",
                    "tags",
                ],
            },
        ),
    )
    return tuple(
        SchemaContract(
            schema_id=schema_id,
            schema_version="1.0",
            media_type=media_type,
            compatibility=compatibility,
            schema_digest=digest_identity(
                {
                    "schemaId": schema_id,
                    "schemaVersion": "1.0",
                    **shape,
                }
            ),
        )
        for schema_id, media_type, compatibility, shape in definitions
    )


def _open_actions() -> tuple[UsageAction, ...]:
    return (
        UsageAction.STORE,
        UsageAction.TRANSFORM,
        UsageAction.DISPLAY,
        UsageAction.SEARCH,
        UsageAction.EXPORT,
        UsageAction.REDISTRIBUTE,
        UsageAction.DERIVE,
        UsageAction.EMBED,
        UsageAction.ML_TRAIN,
        UsageAction.ML_EVALUATE,
    )


def wikidata_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="wikidata-structured-data-cc0",
        policy_version="2026-09-19",
        zone=PolicyZone.OPEN_CC0,
        license_id="CC0-1.0",
        license_uri="https://creativecommons.org/publicdomain/zero/1.0/",
        terms_url="https://www.wikidata.org/wiki/Wikidata:Licensing",
        permissions=_open_actions(),
        audiences=("*",),
        territories=("*",),
        notes=(
            "Applies to Wikidata structured data, not linked Wikimedia Commons "
            "files or text from other Wikimedia projects."
        ),
    )


def musicbrainz_core_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="musicbrainz-core-data-cc0",
        policy_version="2026-10-17",
        zone=PolicyZone.OPEN_CC0,
        license_id="CC0-1.0",
        license_uri="https://creativecommons.org/publicdomain/zero/1.0/",
        terms_url="https://musicbrainz.org/doc/About/Data_License",
        permissions=_open_actions(),
        audiences=("*",),
        territories=("*",),
        attribution_text="MusicBrainz core database contributors",
        notes=(
            "Offline MusicBrainz core dump only. User tags, genres, ratings, "
            "annotations, supplementary/live feeds, and Cover Art Archive assets "
            "are explicitly outside this profile."
        ),
    )


def eidr_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="eidr-public-registry",
        policy_version="2026-09-19",
        zone=PolicyZone.PUBLIC_REGISTRY,
        license_id="EIDR-Public-Registry-Terms",
        terms_url="https://www.eidr.org/our-work",
        permissions=tuple(
            action for action in _open_actions() if action != UsageAction.REDISTRIBUTE
        ),
        audiences=("*",),
        territories=("*",),
        notes=(
            "EIDR records may be used and shared, but the registry may not be "
            "repackaged and presented as a separate proprietary registry."
        ),
    )


def internal_key_continuity_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="internal-key-continuity",
        policy_version="1.0",
        zone=PolicyZone.INTERNAL,
        license_id="PROJECT-INTERNAL",
        terms_url="https://github.com/pandong912/video-media-catalog",
        permissions=_open_actions(),
        audiences=("internal",),
        territories=("*",),
        notes=(
            "Covers project-created stable identity keys and curation metadata, not "
            "the external source facts referenced by those keys."
        ),
    )


def openfootball_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="openfootball-cc0",
        policy_version="2026-10-15",
        zone=PolicyZone.OPEN_CC0,
        license_id="CC0-1.0",
        license_uri="https://creativecommons.org/publicdomain/zero/1.0/",
        terms_url="https://github.com/openfootball",
        permissions=_open_actions(),
        audiences=("*",),
        territories=("*",),
        attribution_text="openfootball contributors",
        notes="Pinned Git snapshots only; provenance retains repository and commit.",
    )


def statsbomb_research_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="statsbomb-open-data-research",
        policy_version="2026-10-15",
        zone=PolicyZone.RESEARCH_PRIVATE,
        license_id="StatsBomb-Open-Data-Terms",
        terms_url="https://github.com/hudl/open-data",
        permissions=(
            UsageAction.STORE,
            UsageAction.TRANSFORM,
            UsageAction.DISPLAY,
            UsageAction.SEARCH,
            UsageAction.DERIVE,
            UsageAction.ML_TRAIN,
            UsageAction.ML_EVALUATE,
        ),
        audiences=("internal",),
        territories=("*",),
        attribution_text="StatsBomb Open Data",
        notes=(
            "Private research use. Match identity may drive exact asset association; "
            "event detail is not a World publication fact."
        ),
    )


def madb_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="madb-public-data-attributed",
        policy_version="2026-10-15",
        zone=PolicyZone.OPEN_ATTRIBUTED,
        license_id="MADB-Terms",
        terms_url="https://mediaarts-db.artmuseums.go.jp/user_terms",
        permissions=_open_actions(),
        audiences=("*",),
        territories=("*",),
        attribution_text=(
            "Media Arts Database, Agency for Cultural Affairs, Japan; modified by "
            "Video World research processing"
        ),
        notes=(
            "Only metadata returned by the reviewed public data interface is in scope; "
            "linked third-party media and rights statements remain excluded."
        ),
    )


def build_community_registry() -> SourceRegistrySnapshot:
    tvmaze_system, tvmaze_product, tvmaze_namespace = tvmaze_registry_entries()
    imdb_system, imdb_product, imdb_namespaces = imdb_registry_entries()
    tmdb_system, tmdb_product, tmdb_namespaces = tmdb_registry_entries()
    europeana_system, europeana_product, europeana_namespace = (
        europeana_registry_entries()
    )
    tvmaze_namespace = tvmaze_namespace.model_copy(
        update={"scheme_aliases": ("tvmaze",)}
    )
    imdb_namespaces = tuple(
        namespace.model_copy(
            update={
                "scheme_aliases": (
                    ("imdb",)
                    if namespace.namespace_id
                    in {
                        IMDB_TITLE_NAMESPACE_ID,
                        IMDB_NAME_NAMESPACE_ID,
                        IMDB_COMPANY_NAMESPACE_ID,
                    }
                    else ()
                )
            }
        )
        for namespace in imdb_namespaces
    )
    tmdb_namespaces = tuple(
        namespace.model_copy(update={"scheme_aliases": (namespace.namespace_id,)})
        for namespace in tmdb_namespaces
    )
    wikidata_system = SourceSystem(
        source_system_id="wikidata",
        name="Wikidata",
        operator="Wikimedia Foundation and Wikidata community",
        homepage="https://www.wikidata.org/",
    )
    musicbrainz_system = SourceSystem(
        source_system_id="musicbrainz",
        name="MusicBrainz",
        operator="MetaBrainz Foundation and MusicBrainz community",
        homepage="https://musicbrainz.org/",
    )
    eidr_system = SourceSystem(
        source_system_id="eidr",
        name="Entertainment Identifier Registry",
        operator="EIDR Association",
        homepage="https://www.eidr.org/",
    )
    internal_system = SourceSystem(
        source_system_id="video-media-catalog",
        name="Video Media Catalog",
        operator="video-media-catalog project",
        homepage="https://github.com/pandong912/video-media-catalog",
    )
    openfootball_system = SourceSystem(
        source_system_id="openfootball",
        name="openfootball",
        operator="openfootball contributors",
        homepage="https://github.com/openfootball",
    )
    statsbomb_system = SourceSystem(
        source_system_id="statsbomb",
        name="StatsBomb Open Data",
        operator="StatsBomb",
        homepage="https://github.com/hudl/open-data",
    )
    madb_system = SourceSystem(
        source_system_id="madb",
        name="Media Arts Database",
        operator="Agency for Cultural Affairs, Japan",
        homepage="https://mediaarts-db.artmuseums.go.jp/",
    )
    wikidata_product = SourceProduct(
        source_product_id="wikidata-json-dump",
        source_system_id="wikidata",
        name="Wikidata JSON entity dump",
        kind=SourceProductKind.KNOWLEDGE_GRAPH,
        policy_id="wikidata-structured-data-cc0",
        connector_ids=(WIKIDATA_FULL_MEDIA_CONNECTOR_ID,),
        schema_contract_ids=(
            "wikidata-research-entity",
            "wikidata-global-reference-entity",
        ),
        documentation_url=("https://www.wikidata.org/wiki/Wikidata:Database_download"),
    )
    musicbrainz_product = SourceProduct(
        source_product_id="musicbrainz-core-dump",
        source_system_id="musicbrainz",
        name="MusicBrainz core database dump",
        kind=SourceProductKind.COMMUNITY_DATABASE,
        policy_id="musicbrainz-core-data-cc0",
        connector_ids=(MUSICBRAINZ_CORE_DUMP_CONNECTOR_ID,),
        schema_contract_ids=("musicbrainz-core-entity",),
        documentation_url="https://musicbrainz.org/doc/MusicBrainz_Database/Download",
    )
    eidr_product = SourceProduct(
        source_product_id="eidr-public-registry",
        source_system_id="eidr",
        name="EIDR public registry records",
        kind=SourceProductKind.IDENTIFIER_REGISTRY,
        policy_id="eidr-public-registry",
        connector_ids=(EIDR_EXACT_LOOKUP_CONNECTOR_ID,),
        documentation_url="https://www.eidr.org/faq",
    )
    identity_product = SourceProduct(
        source_product_id="identity-resolution-v2",
        source_system_id="video-media-catalog",
        name="Community identity resolution v2",
        kind=SourceProductKind.INTERNAL_CATALOG,
        policy_id="internal-key-continuity",
        connector_ids=("community-identity-spark-v2",),
        documentation_url=("https://github.com/pandong912/video-media-catalog"),
    )
    openfootball_product = SourceProduct(
        source_product_id="openfootball-git",
        source_system_id="openfootball",
        name="openfootball pinned repository snapshots",
        kind=SourceProductKind.COMMUNITY_DATABASE,
        policy_id="openfootball-cc0",
        connector_ids=(OPENFOOTBALL_SNAPSHOT_CONNECTOR_ID,),
        schema_contract_ids=("openfootball-event",),
        documentation_url="https://github.com/openfootball",
    )
    statsbomb_product = SourceProduct(
        source_product_id="statsbomb-open-data",
        source_system_id="statsbomb",
        name="StatsBomb Open Data pinned snapshots",
        kind=SourceProductKind.COMMUNITY_DATABASE,
        policy_id="statsbomb-open-data-research",
        connector_ids=(STATSBOMB_SNAPSHOT_CONNECTOR_ID,),
        schema_contract_ids=("statsbomb-open-data",),
        documentation_url="https://github.com/hudl/open-data",
    )
    madb_product = SourceProduct(
        source_product_id="madb-public-data",
        source_system_id="madb",
        name="Media Arts Database public metadata snapshots",
        kind=SourceProductKind.GOVERNMENT_DATA,
        policy_id="madb-public-data-attributed",
        connector_ids=(MADB_SNAPSHOT_CONNECTOR_ID,),
        schema_contract_ids=("madb-animation-record",),
        documentation_url="https://mediaarts-db.artmuseums.go.jp/",
    )
    return SourceRegistrySnapshot(
        registry_id="community-catalog-bootstrap",
        source_systems=(
            wikidata_system,
            musicbrainz_system,
            eidr_system,
            internal_system,
            tvmaze_system,
            imdb_system,
            tmdb_system,
            europeana_system,
            openfootball_system,
            statsbomb_system,
            madb_system,
        ),
        source_products=(
            wikidata_product,
            musicbrainz_product,
            eidr_product,
            identity_product,
            tvmaze_product,
            imdb_product,
            tmdb_product,
            europeana_product,
            openfootball_product,
            statsbomb_product,
            madb_product,
        ),
        source_namespaces=(
            SourceNamespace(
                namespace_id="wikidata-item",
                source_product_id="wikidata-json-dump",
                issuer="Wikidata",
                referent_kinds=(
                    "EDITORIAL_WORK",
                    "SERIES",
                    "SEASON",
                    "EPISODE",
                    "AGENT",
                    "ORGANIZATION",
                    "SPORTS_COMPETITION",
                    "SPORTS_SEASON",
                    "SPORTS_STAGE",
                    "SPORTS_EVENT",
                    "SPORTS_PARTICIPANT",
                    "SPORTS_ENTRY",
                    "SPORTS_VENUE",
                    "CHARACTER",
                    "NEWS_PROGRAM",
                    "NEWS_EDITION",
                    "NEWS_STORY",
                    "REPORTED_EVENT",
                    "INFORMATION_SOURCE",
                    "CLAIM",
                    "CORRECTION",
                    "COURSE",
                    "MODULE",
                    "LESSON",
                    "LEARNING_CONCEPT",
                    "LEARNING_OBJECTIVE",
                    "PREREQUISITE",
                    "PROCEDURE_STEP",
                    "DEMONSTRATION",
                    "MUSIC_ARTIST",
                    "MUSICAL_WORK",
                    "MUSIC_RECORDING",
                    "MUSIC_RELEASE_GROUP",
                    "MUSIC_RELEASE",
                ),
                scheme_aliases=("wikidata",),
                identifier_pattern=r"Q[1-9][0-9]*",
            ),
            *(
                SourceNamespace(
                    namespace_id=f"musicbrainz-{suffix}",
                    source_product_id="musicbrainz-core-dump",
                    issuer="MusicBrainz",
                    referent_kinds=(referent_kind,),
                    identifier_pattern=(
                        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                        r"[0-9a-f]{4}-[0-9a-f]{12}"
                    ),
                )
                for suffix, referent_kind in (
                    ("artist", "MUSIC_ARTIST"),
                    ("work", "MUSICAL_WORK"),
                    ("recording", "MUSIC_RECORDING"),
                    ("release-group", "MUSIC_RELEASE_GROUP"),
                    ("release", "MUSIC_RELEASE"),
                )
            ),
            SourceNamespace(
                namespace_id="isrc",
                source_product_id="musicbrainz-core-dump",
                issuer="IFPI",
                referent_kinds=("MUSIC_RECORDING",),
                identifier_pattern=r"[A-Z]{2}[A-Z0-9]{3}[0-9]{7}",
                case_sensitive=False,
            ),
            SourceNamespace(
                namespace_id="iswc",
                source_product_id="musicbrainz-core-dump",
                issuer="CISAC",
                referent_kinds=("MUSICAL_WORK",),
                identifier_pattern=r"T-[0-9]{3}\.[0-9]{3}\.[0-9]{3}-[0-9]",
                case_sensitive=False,
            ),
            SourceNamespace(
                namespace_id="eidr-content",
                source_product_id="eidr-public-registry",
                issuer="EIDR Association",
                referent_kinds=(
                    "EDITORIAL_WORK",
                    "SERIES",
                    "SEASON",
                    "EPISODE",
                    "EDIT",
                    "MANIFESTATION",
                ),
                scheme_aliases=("eidr",),
                identifier_pattern=(r"10\.5240/(?:[0-9A-Z]{4}-){5}[0-9A-Z]"),
                case_sensitive=False,
            ),
            tvmaze_namespace,
            SourceNamespace(
                namespace_id=DOUBAN_WORK_NAMESPACE_ID,
                source_product_id="wikidata-json-dump",
                issuer="Douban",
                referent_kinds=(
                    "EDITORIAL_WORK",
                    "SERIES",
                    "SEASON",
                    "EPISODE",
                ),
                scheme_aliases=(
                    DOUBAN_LEGACY_NAMESPACE_ID,
                    DOUBAN_LEGACY_SCHEME,
                ),
                identifier_pattern=DOUBAN_IDENTIFIER_PATTERN,
            ),
            SourceNamespace(
                namespace_id=DOUBAN_PERSON_NAMESPACE_ID,
                source_product_id="wikidata-json-dump",
                issuer="Douban",
                referent_kinds=("AGENT",),
                scheme_aliases=(
                    DOUBAN_LEGACY_NAMESPACE_ID,
                    DOUBAN_LEGACY_SCHEME,
                ),
                identifier_pattern=DOUBAN_IDENTIFIER_PATTERN,
            ),
            SourceNamespace(
                namespace_id="eidr-alternate",
                source_product_id="eidr-public-registry",
                issuer="EIDR Association",
                referent_kinds=(
                    "EDITORIAL_WORK",
                    "SERIES",
                    "SEASON",
                    "EPISODE",
                    "EDIT",
                ),
            ),
            SourceNamespace(
                namespace_id="thetvdb-series",
                source_product_id="tvmaze-public-api",
                issuer="TheTVDB",
                referent_kinds=("SERIES",),
                scheme_aliases=("thetvdb",),
                identifier_pattern=r"[1-9][0-9]*",
            ),
            SourceNamespace(
                namespace_id="tvrage-show",
                source_product_id="tvmaze-public-api",
                issuer="TVRage",
                referent_kinds=("SERIES",),
                identifier_pattern=r"[1-9][0-9]*",
            ),
            *imdb_namespaces,
            *tmdb_namespaces,
            europeana_namespace,
            SourceNamespace(
                namespace_id="openfootball-competition",
                source_product_id="openfootball-git",
                issuer="openfootball",
                referent_kinds=("SPORTS_COMPETITION",),
                identifier_pattern=r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}",
            ),
            SourceNamespace(
                namespace_id="openfootball-event",
                source_product_id="openfootball-git",
                issuer="openfootball",
                referent_kinds=("SPORTS_EVENT",),
                identifier_pattern=r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}",
            ),
            SourceNamespace(
                namespace_id="openfootball-team",
                source_product_id="openfootball-git",
                issuer="openfootball",
                referent_kinds=("SPORTS_PARTICIPANT",),
                identifier_pattern=r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}",
            ),
            *(
                SourceNamespace(
                    namespace_id=f"openfootball-{suffix}",
                    source_product_id="openfootball-git",
                    issuer="openfootball",
                    referent_kinds=(referent_kind,),
                    identifier_pattern=r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}",
                )
                for suffix, referent_kind in (
                    ("season", "SPORTS_SEASON"),
                    ("stage", "SPORTS_STAGE"),
                    ("entry", "SPORTS_ENTRY"),
                    ("venue", "SPORTS_VENUE"),
                )
            ),
            SourceNamespace(
                namespace_id="statsbomb-match",
                source_product_id="statsbomb-open-data",
                issuer="StatsBomb",
                referent_kinds=("SPORTS_EVENT",),
                identifier_pattern=r"[1-9][0-9]*",
            ),
            *(
                SourceNamespace(
                    namespace_id=f"statsbomb-{suffix}",
                    source_product_id="statsbomb-open-data",
                    issuer="StatsBomb",
                    referent_kinds=(referent_kind,),
                    identifier_pattern=r"[1-9][0-9]*",
                )
                for suffix, referent_kind in (
                    ("competition", "SPORTS_COMPETITION"),
                    ("season", "SPORTS_SEASON"),
                    ("team", "SPORTS_PARTICIPANT"),
                    ("player", "SPORTS_PARTICIPANT"),
                    ("venue", "SPORTS_VENUE"),
                )
            ),
            SourceNamespace(
                namespace_id="madb-work",
                source_product_id="madb-public-data",
                issuer="Media Arts Database",
                referent_kinds=(
                    "EDITORIAL_WORK",
                    "SERIES",
                    "SEASON",
                    "EPISODE",
                ),
                identifier_pattern=r"\S{1,256}",
            ),
            SourceNamespace(
                namespace_id="madb-character",
                source_product_id="madb-public-data",
                issuer="Media Arts Database",
                referent_kinds=("CHARACTER",),
                identifier_pattern=r"\S{1,256}",
            ),
            SourceNamespace(
                namespace_id="madb-agent",
                source_product_id="madb-public-data",
                issuer="Media Arts Database",
                referent_kinds=("AGENT", "ORGANIZATION"),
                identifier_pattern=r"\S{1,256}",
            ),
        ),
        schema_contracts=research_source_schema_contracts(),
        rights_profiles=(
            wikidata_rights_profile(),
            musicbrainz_core_rights_profile(),
            eidr_rights_profile(),
            internal_key_continuity_profile(),
            tvmaze_rights_profile(),
            imdb_rights_profile(),
            tmdb_rights_profile(),
            europeana_metadata_rights_profile(),
            openfootball_rights_profile(),
            statsbomb_research_rights_profile(),
            madb_rights_profile(),
        ),
    )
