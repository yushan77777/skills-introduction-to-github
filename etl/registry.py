"""Presentation metadata for the discovered ETL jobs.

Discovery (:mod:`etl.discovery`) answers *what* can run and *what it needs*.
This module only answers *how to ask for it*: labels, input types, grouping,
help text and format hints for parameter names of
``etl_table_manual.run_etl``.

The split matters. Nothing here decides whether a job exists or whether a
parameter is required — those come from the source every time the file changes.
If a new ETL is added to ``run_etl`` and nobody touches this file, the job still
appears and still runs; it just gets generic labels and a note saying so.

No value in this module is a credential, a host or a path: placeholders are
illustrative only.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .discovery import Discovery, cached_discovery


@dataclass
class FieldSpec:
    """How one ``run_etl`` keyword argument is presented in the form."""

    name: str
    label: str
    type: str = "text"          # text | password | textarea | number | select | checkbox
    group: str = "Source"       # Source | Extraction | Destination | Advanced
    help: str = ""
    placeholder: str = ""
    options: list[dict] | None = None
    #: Fill the dropdown from config.yaml keys: "oracle" | "greenplum".
    options_from: str | None = None
    sensitive: bool = False
    #: Checked on Validate: input_file | input_dir | output_file | output_dir
    path_kind: str | None = None
    #: Regex enforced in the browser and again on the server.
    pattern: str | None = None
    pattern_hint: str = ""
    #: ``{"field": ..., "equals": ...}`` — only shown and sent when it holds.
    depends_on: dict | None = None
    #: Rendered in the form but never passed to ``run_etl``.
    ui_only: bool = False


def _spec(name: str, label: str, **kw: Any) -> tuple[str, FieldSpec]:
    return name, FieldSpec(name=name, label=label, **kw)


_WRITE_MODES = [{"value": v, "label": v} for v in ("overwrite", "append", "error", "ignore")]

#: Metadata for every ``run_etl`` keyword argument the supplied source declares.
FIELD_SPECS: dict[str, FieldSpec] = dict([
    _spec("oracle_config_name", "Oracle config key", type="select", group="Source",
          options_from="oracle",
          help="Top-level key in config/config.yaml holding url, user, encrypted "
               "password, driver and pickle. Credentials stay in the file."),
    _spec("query", "Extraction query", type="textarea", group="Extraction",
          placeholder="SELECT * FROM SCHEMA.TABLE WHERE TRAN_DATE >= DATE '2026-01-01'",
          help="Wrapped by the ETL as ( … ) subq. Do not end it with a semicolon."),
    _spec("_column", "Partition column", group="Extraction",
          placeholder="ACCOUNT_NO",
          help="Used to build 20 ORA_HASH predicates for a parallel JDBC read. "
               "Pick a high-cardinality, indexed column."),
    _spec("csv_input", "Source CSV", group="Source", path_kind="input_file",
          placeholder="/data/imports/customers.csv",
          help="A file, or a Spark-readable directory of CSV parts. The first "
               "row is treated as the header and types are inferred."),
    _spec("pqt_input", "Source Parquet", group="Source", path_kind="input_file",
          placeholder="/data/imports/customers.parquet",
          help="Read into memory by pandas — size it against the memory "
               "available on the ETL host."),
    _spec("output_path", "Output path", group="Destination", path_kind="output_file",
          placeholder="/data/exports/customers.csv",
          help="Full path of the file (or directory) the ETL produces."),
    _spec("tmp_dir", "Spark staging directory", group="Destination",
          path_kind="output_dir", placeholder="/data/exports/_tmp_customers",
          help="Spark writes part files here first. Use a directory that holds "
               "nothing else — the ETL cleans it out afterwards."),
    _spec("parq_name", "Dataset file name", group="Destination",
          placeholder="customers.parquet",
          help="Reported back in the result. The implementation writes Spark "
               "part files into the output directory; this name labels the "
               "dataset rather than merging it into a single file."),
    _spec("gp_url", "Greenplum JDBC URL", group="Destination",
          placeholder="jdbc:postgresql://host:5432/database",
          pattern=r"^jdbc:postgresql://[^/\s]+/\S+$",
          pattern_hint="Expected jdbc:postgresql://host:port/database"),
    _spec("gp_table", "Target table", group="Destination",
          placeholder="schema.table_name",
          pattern=r"^[A-Za-z_][\w$]*(\.[A-Za-z_][\w$]*)?$",
          pattern_hint="Use schema.table or table"),
    _spec("gp_user", "Greenplum user", group="Destination"),
    _spec("gp_password", "Greenplum password", type="password",
          group="Destination", sensitive=True,
          help="Held in memory for this run only. Never logged, never written "
               "to disk, never echoed back to the browser."),
    _spec("gp_config_key", "Greenplum config key", type="select",
          group="Destination", options_from="greenplum",
          help="Passed to the greenplum connector as its config block. "
               "Credentials come from config.yaml, not from this form."),
    _spec("gp_server_port", "gpfdist port range", group="Advanced",
          pattern=r"^\d{1,5}(-\d{1,5})?$",
          pattern_hint="A port or a port range, e.g. 32768-42768",
          help="Ports the Greenplum connector opens for gpfdist sessions."),
    _spec("gp_mode", "Write mode", type="select", group="Advanced",
          options=_WRITE_MODES),
    _spec("gp_truncate", "Truncate instead of drop", type="select", group="Advanced",
          options=[{"value": "true", "label": "true"},
                   {"value": "false", "label": "false"}],
          help="Connector option: keep the table definition and truncate it "
               "rather than dropping and recreating it."),
    _spec("p_column", "Partition column", group="Extraction", placeholder="id",
          depends_on={"field": "use_partitioning", "equals": True},
          help="Numeric or date column split across 50 JDBC partitions."),
    _spec("lower_bound", "Lower bound", type="number", group="Extraction",
          depends_on={"field": "use_partitioning", "equals": True}),
    _spec("upper_bound", "Upper bound", type="number", group="Extraction",
          depends_on={"field": "use_partitioning", "equals": True}),
    _spec("oracle_jdbc_url", "Oracle JDBC URL", group="Destination",
          placeholder="jdbc:oracle:thin:@//host:1521/service",
          pattern=r"^jdbc:oracle:thin:@\S+$",
          pattern_hint="Expected jdbc:oracle:thin:@//host:port/service"),
    _spec("oracle_table", "Target table", group="Destination",
          placeholder="SCHEMA.TABLE_NAME",
          pattern=r"^[A-Za-z_][\w$]*(\.[A-Za-z_][\w$]*)?$",
          pattern_hint="Use SCHEMA.TABLE or TABLE"),
    _spec("oracle_user", "Oracle user", group="Destination"),
    _spec("oracle_password", "Oracle password", type="password",
          group="Destination", sensitive=True,
          help="This ETL takes credentials from the form rather than from "
               "config.yaml. The value is never logged or saved."),
    _spec("oracle_driver", "JDBC driver class", group="Advanced"),
    _spec("oracle_mode", "Write mode", type="select", group="Advanced",
          options=_WRITE_MODES),
    _spec("compression", "Parquet compression", type="select", group="Advanced",
          options=[{"value": v, "label": v} for v in
                   ("snappy", "gzip", "zstd", "none")]),
    _spec("base_dir", "Base directory", group="Advanced",
          help="Passed straight through to run_etl. The supplied implementation "
               "accepts it but never reads it; it defaults to the ETL project root."),
    _spec("oracle_target", "Oracle target key", group="Advanced",
          help="Accepted by run_etl but unused by the supplied implementation "
               "(the block that read it is commented out)."),
])

#: A UI-only switch: the implementation takes its single-connection branch only
#: when ``p_column`` is the empty string, so the form sends exactly that.
_USE_PARTITIONING = FieldSpec(
    name="use_partitioning", label="Read in parallel partitions", type="checkbox",
    group="Extraction", ui_only=True,
    help="Off sends an empty partition column, which is the branch the ETL "
         "treats as a single-connection read.",
)


@dataclass
class JobMeta:
    """Human-facing description of one discovered job."""

    label: str
    source: str
    destination: str
    description: str
    #: Parameter names to show, in order. Discovery decides which are required.
    fields: list[str] = field(default_factory=list)
    #: Parameters the implementation needs but does not guard with a
    #: ValueError, so discovery cannot see them. Each one is justified in
    #: ``notes`` — never add a name here without saying why.
    extra_required: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


_COLUMN_NOTE = (
    "run_etl does not guard _column, but read_oracle_df interpolates it into "
    "20 MOD(ORA_HASH(<column>), 20) = n predicates — leaving it empty produces "
    "invalid SQL rather than a clear error, so the form requires it."
)

_ORACLE_SOURCE = ["oracle_config_name", "query", "_column"]
_GP_WRITE = ["gp_url", "gp_table", "gp_user", "gp_password",
             "gp_server_port", "gp_mode", "gp_truncate"]

#: Keyed by the job identifiers discovery finds in the source.
JOB_META: dict[str, JobMeta] = {
    "oracle_to_csv": JobMeta(
        label="Oracle → CSV", source="Oracle", destination="CSV file",
        description="Parallel JDBC read from Oracle, null bytes stripped and "
                    "column names lower-cased, written as one CSV file.",
        fields=[*_ORACLE_SOURCE, "output_path", "tmp_dir", "base_dir"],
        extra_required=["_column"],
        notes=[_COLUMN_NOTE],
    ),
    "oracle_to_parquet": JobMeta(
        label="Oracle → Parquet", source="Oracle", destination="Parquet file",
        description="The same Oracle read as Oracle → CSV, written as one "
                    "Parquet file.",
        fields=[*_ORACLE_SOURCE, "output_path", "tmp_dir", "base_dir"],
        extra_required=["_column"],
        notes=[_COLUMN_NOTE],
    ),
    "oracle_to_greenplum": JobMeta(
        label="Oracle → Greenplum", source="Oracle", destination="Greenplum",
        description="Reads Oracle in 20 ORA_HASH partitions and writes straight "
                    "to Greenplum through the gpfdist connector.",
        fields=[*_ORACLE_SOURCE, *_GP_WRITE, "base_dir"],
        extra_required=["_column"],
        notes=[_COLUMN_NOTE],
    ),
    "csv_to_greenplum": JobMeta(
        label="CSV → Greenplum", source="CSV file", destination="Greenplum",
        description="Reads a header CSV with schema inference and writes it to "
                    "Greenplum through the gpfdist connector.",
        fields=["csv_input", "gp_config_key", *_GP_WRITE, "base_dir"],
        extra_required=["gp_url"],
        notes=["run_etl validates csv_input, gp_table and gp_config_key, but the "
               "live write path uses gp_url, gp_user and gp_password — the "
               "config-key branch (pandas copy_from_dataframe) is commented out "
               "in the implementation. The form therefore asks for both."],
    ),
    "parquet_to_greenplum": JobMeta(
        label="Parquet → Greenplum", source="Parquet file", destination="Greenplum",
        description="Loads Parquet with pandas and bulk-copies it through the "
                    "existing greenplum connector. Returns the row count.",
        fields=["pqt_input", "gp_config_key", "gp_table", "base_dir"],
        notes=["The only job that reads Greenplum credentials from config.yaml "
               "rather than from this form.",
               "The implementation starts a Spark session it never uses, and "
               "ticks one more step than its STEP_MAP declares, so the step "
               "strip can read 4 of 3 near the end."],
    ),
    "csv_to_oracle": JobMeta(
        label="CSV → Oracle", source="CSV file", destination="Oracle",
        description="Reads a header CSV with schema inference and writes it to "
                    "Oracle over JDBC in batches of 50,000 rows.",
        fields=["csv_input", "oracle_jdbc_url", "oracle_table", "oracle_user",
                "oracle_password", "oracle_driver", "oracle_mode", "base_dir"],
    ),
    "greenplum_read_to_parquet": JobMeta(
        label="Greenplum → Parquet", source="Greenplum",
        destination="Parquet directory",
        description="JDBC read from Greenplum, optionally range-partitioned, "
                    "written as a Parquet dataset directory.",
        fields=["gp_url", "gp_user", "gp_password", "query", "use_partitioning",
                "p_column", "lower_bound", "upper_bound", "output_path",
                "parq_name", "compression", "base_dir"],
        notes=["Leaving parallel reads off is the safe default: the "
               "implementation takes the single-connection branch only when the "
               "partition column is an empty string, and the form sends exactly "
               "that."],
    ),
}

#: Overrides for fields whose meaning shifts between jobs.
_PER_JOB_OVERRIDES: dict[tuple[str, str], dict[str, Any]] = {
    ("oracle_to_csv", "output_path"): {
        "label": "Final CSV file", "placeholder": "/data/exports/customers.csv"},
    ("oracle_to_parquet", "output_path"): {
        "label": "Final Parquet file",
        "placeholder": "/data/exports/customers.parquet"},
    ("oracle_to_parquet", "tmp_dir"): {
        "help": "Removed entirely once the single Parquet file is moved out."},
    ("greenplum_read_to_parquet", "output_path"): {
        "label": "Parquet output directory", "path_kind": "output_dir",
        "placeholder": "/data/exports/customers_parquet",
        "help": "Overwritten on every run."},
    ("greenplum_read_to_parquet", "gp_url"): {"group": "Source"},
    ("greenplum_read_to_parquet", "gp_user"): {"group": "Source"},
    ("greenplum_read_to_parquet", "gp_password"): {"group": "Source"},
    ("csv_to_greenplum", "gp_config_key"): {
        "group": "Source",
        "help": "run_etl refuses to start this job without the key, although "
                "the active write path uses the JDBC URL below."},
}

GROUP_ORDER = ["Source", "Extraction", "Destination", "Advanced"]


def _titleise(key: str) -> str:
    """Readable label for a job the metadata above has never seen."""
    parts = key.split("_to_")
    if len(parts) == 2:
        return f"{parts[0].replace('_', ' ').title()} → {parts[1].replace('_', ' ').title()}"
    return key.replace("_", " ").title()


def _field_for(job_key: str, name: str, discovery: Discovery) -> dict:
    """One form field: spec metadata + the signature default + required-ness."""
    if name == "use_partitioning":
        spec = _USE_PARTITIONING
    else:
        spec = FIELD_SPECS.get(name) or FieldSpec(
            name=name,
            label=name.replace("_", " ").strip().capitalize(),
            group="Advanced",
            help="Discovered in the entry point signature; no description is "
                 "registered for it yet.",
        )
    data = asdict(spec)
    for key, value in _PER_JOB_OVERRIDES.get((job_key, name), {}).items():
        data[key] = value

    job = discovery.jobs.get(job_key)
    meta = JOB_META.get(job_key)
    required = set(job.runtime_required) if job else set()
    if meta:
        required |= set(meta.extra_required)
    data["required"] = name in required
    data["required_by_source"] = bool(job and name in job.runtime_required)
    default = discovery.parameters.get(name)
    if name == "base_dir" and default in (None, ""):
        default = None  # filled in by the API from the project root
    data["default"] = None if spec.ui_only else default
    if spec.ui_only:
        data["default"] = False if spec.type == "checkbox" else None
    data["known"] = name in discovery.parameters or spec.ui_only
    return data


def build_job(job_key: str, discovery: Discovery | None = None) -> dict:
    """The full UI description of one discovered job."""
    discovery = discovery or cached_discovery()
    job = discovery.jobs.get(job_key)
    if job is None:
        raise KeyError(f"Unknown ETL job '{job_key}'.")

    meta = JOB_META.get(job_key)
    notes = list(meta.notes) if meta else []
    if meta is None:
        notes.append(
            "This job was discovered in the ETL source but has no UI description "
            "registered yet, so only the parameters it refuses to start without "
            "are shown. Add an entry to etl/registry.py to expose the rest."
        )
        names = list(job.runtime_required)
    else:
        names = [n for n in meta.fields]
        missing = [n for n in job.runtime_required if n not in names]
        if missing:
            notes.append(
                "The ETL source requires "
                + ", ".join(missing)
                + " for this job but etl/registry.py does not list it; it has "
                  "been added to the form automatically."
            )
            names += missing

    fields = [_field_for(job_key, n, discovery) for n in names]
    fields.sort(key=lambda f: GROUP_ORDER.index(f["group"])
                if f["group"] in GROUP_ORDER else len(GROUP_ORDER))

    return {
        "key": job_key,
        "label": meta.label if meta else _titleise(job_key),
        "source": meta.source if meta else "—",
        "destination": meta.destination if meta else "—",
        "description": meta.description if meta else
                       "Discovered in the ETL source. No description registered.",
        "steps": list(job.steps),
        "runtime_required": list(job.runtime_required),
        "available": job.implemented and job.declared,
        "notes": notes,
        "fields": fields,
    }


def list_jobs(discovery: Discovery | None = None) -> list[dict]:
    """Every discovered job, runnable ones first."""
    discovery = discovery or cached_discovery()
    jobs = [build_job(key, discovery) for key in discovery.jobs]
    jobs.sort(key=lambda j: (not j["available"], j["label"]))
    return jobs


def job_fields(job_key: str, discovery: Discovery | None = None) -> list[dict]:
    return build_job(job_key, discovery)["fields"]


def sensitive_field_names(job_key: str | None = None) -> set[str]:
    """Fields treated as credentials.

    Without a job key this is every sensitive name anywhere in the specs — used
    when writing to disk, so a mis-addressed payload cannot smuggle one job's
    password into another job's record.
    """
    if job_key is None:
        return {s.name for s in FIELD_SPECS.values() if s.sensitive}
    return {f["name"] for f in job_fields(job_key) if f.get("sensitive")}


def active_fields(job_key: str, values: dict) -> list[dict]:
    """Fields whose ``depends_on`` condition is satisfied by ``values``."""
    out = []
    for f in job_fields(job_key):
        dep = f.get("depends_on")
        if dep and values.get(dep["field"]) != dep["equals"]:
            continue
        out.append(f)
    return out


def to_run_etl_kwargs(job_key: str, values: dict) -> dict:
    """Validated form values -> the exact kwargs ``run_etl`` expects."""
    kwargs: dict = {"etl_name": job_key}
    active = {f["name"] for f in active_fields(job_key, values)}

    for f in job_fields(job_key):
        name = f["name"]
        if f.get("ui_only") or name not in active or not f.get("known"):
            continue
        value = values.get(name)
        if value is None or value == "":
            if f.get("default") not in (None, ""):
                value = f["default"]
            elif not f.get("required"):
                continue
        kwargs[name] = value

    # greenplum_read_to_parquet compares p_column to "" to choose the
    # single-connection branch, so send "" rather than omitting it.
    if "p_column" in FIELD_SPECS and not values.get("use_partitioning"):
        if any(f["name"] == "p_column" for f in job_fields(job_key)):
            kwargs["p_column"] = ""
            kwargs.pop("lower_bound", None)
            kwargs.pop("upper_bound", None)

    return kwargs
