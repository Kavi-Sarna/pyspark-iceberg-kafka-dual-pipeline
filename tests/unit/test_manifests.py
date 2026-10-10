"""The Kubernetes manifests, checked against the code they deploy.

A manifest is configuration that nothing compiles and nothing imports, so the
usual tools say nothing about it. These are the invariants that would
otherwise only fail on a cluster, minutes into a run: a package coordinate
that drifted from session.py, a Service renamed out from under the config, a
path that no longer exists in the image.

Schema validity is a separate question, answered by `make k8s-validate`
against the published Kubernetes and operator schemas. This file is about
agreement between the manifests and this repository.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from pipeline.config import AppConfig
from pipeline.main import parse_args
from pipeline.session import PACKAGES, SPARK_VERSION, STREAM_PACKAGES

REPO = Path(__file__).resolve().parents[2]
K8S = REPO / "k8s"
NAMESPACE = "pipeline"

# The long options the entry point accepts, taken from the parser rather than
# restated. Every destination is one flag.
FLAGS = {"--" + name.replace("_", "-") for name in vars(parse_args([]))}


def dockerfile_python() -> str:
    """The Python version uv installs in the image, as the Dockerfile says."""
    match = re.search(r"uv python install (\d+\.\d+)", (REPO / "docker" / "Dockerfile").read_text())
    assert match, "the Dockerfile no longer pins a Python version"
    return match.group(1)


def load(name: str) -> list[dict[str, Any]]:
    return [d for d in yaml.safe_load_all((K8S / name).read_text()) if d]


def one(name: str) -> dict[str, Any]:
    docs = load(name)
    assert len(docs) == 1, f"{name} should hold a single resource"
    return docs[0]


@pytest.fixture(scope="module")
def stream() -> dict[str, Any]:
    return one("30-stream.yaml")


@pytest.fixture(scope="module")
def batch() -> dict[str, Any]:
    return one("40-batch.yaml")


@pytest.fixture(scope="module")
def everything() -> list[dict[str, Any]]:
    return [d for f in sorted(K8S.glob("*.yaml")) if f.name != "kind.yaml" for d in load(f.name)]


def containers(doc: dict[str, Any]) -> list[dict[str, Any]]:
    spec = doc.get("spec", {})
    pod = spec.get("template", {}).get("spec") if doc["kind"] in ("Deployment", "Job") else None
    return list(pod.get("containers", [])) if pod else []


# --- agreement with the code -----------------------------------------------


def test_the_streaming_job_asks_for_the_jars_the_code_expects(stream: dict[str, Any]) -> None:
    """The operator runs spark-submit, where spark.jars.packages set from the
    session builder arrives too late to matter. deps.packages is the only
    place these can be stated, so it is the place they can silently drift."""
    wanted = set(PACKAGES.split(",")) | set(STREAM_PACKAGES.split(","))
    assert set(stream["spec"]["deps"]["packages"]) == wanted


def test_the_batch_job_asks_for_iceberg_and_not_kafka(batch: dict[str, Any]) -> None:
    """A batch run reads Iceberg. Pulling the Kafka and Avro connectors it
    never calls would be two more downloads before every scheduled run."""
    assert set(batch["spec"]["template"]["deps"]["packages"]) == set(PACKAGES.split(","))


@pytest.mark.parametrize("fixture", ["stream", "batch"])
def test_the_spark_version_matches_the_image(fixture: str, request: pytest.FixtureRequest) -> None:
    doc = request.getfixturevalue(fixture)
    spec = doc["spec"].get("template", doc["spec"])
    assert spec["sparkVersion"] == SPARK_VERSION


@pytest.mark.parametrize("fixture", ["stream", "batch"])
def test_the_entry_point_is_where_a_no_editable_install_puts_it(
    fixture: str, request: pytest.FixtureRequest
) -> None:
    """local:// means inside the image. The path follows from installing the
    package rather than pointing at /app/src, which is what the Dockerfile
    does and what makes the venv alone sufficient at runtime."""
    doc = request.getfixturevalue(fixture)
    spec = doc["spec"].get("template", doc["spec"])
    # Derived, not restated: bumping Python in the Dockerfile and not here
    # fails at submit time with a file that is not found, and nothing run
    # locally would have said so.
    assert spec["mainApplicationFile"] == (
        f"local:///app/.venv/lib/python{dockerfile_python()}/site-packages/pipeline/main.py"
    )


def test_the_image_python_matches_the_one_this_project_requires() -> None:
    """Three files name a Python version. The test above ties two together;
    this is the third."""
    version = dockerfile_python()
    assert version == f"{sys.version_info.major}.{sys.version_info.minor}"
    assert f'requires-python = ">={version}' in (REPO / "pyproject.toml").read_text()


def test_the_configured_checkpoint_lands_inside_the_mounted_volume(
    stream: dict[str, Any],
) -> None:
    """A checkpoint written outside the PVC is lost with the driver pod, and
    the query silently reprocesses from the beginning on restart.

    The path comes from app.toml, so this compares the manifest with the
    configuration the job will really read rather than with itself."""
    spec = stream["spec"]
    checkpoint = AppConfig.load(env={}).stream.checkpoint
    mounts = [m["mountPath"] for m in spec["driver"]["volumeMounts"]]
    assert any(checkpoint.startswith(m) for m in mounts), (checkpoint, mounts)
    # Executors write the state store, so they need it too.
    assert spec["executor"]["volumeMounts"] == spec["driver"]["volumeMounts"]


@pytest.mark.parametrize("fixture", ["stream", "batch"])
def test_the_arguments_are_ones_the_entry_point_accepts(
    fixture: str, request: pytest.FixtureRequest
) -> None:
    """argparse exits 2 on an unknown flag, before a SparkSession exists.
    Under restartPolicy Always that is a crash loop, and every schema in the
    world validates the manifest happily, because to them it is one more
    string in a list.

    A manifest can only be checked against the program it launches."""
    doc = request.getfixturevalue(fixture)
    spec = doc["spec"].get("template", doc["spec"])
    parse_args(spec.get("arguments", []))


def test_compose_passes_flags_the_entry_point_accepts() -> None:
    """Compose builds its own command line and carried a flag that had been
    removed for three PRs, because nothing compared it with the parser."""
    compose = yaml.safe_load((REPO / "docker" / "compose.yaml").read_text())
    checked = 0
    for name, service in compose["services"].items():
        words = str(service.get("command", "")).split()
        if "--mode" not in words:
            continue
        # spark-submit's own flags come before the application file, and
        # --mode is the first one only this program understands.
        checked += 1
        for word in words[words.index("--mode") :]:
            if word.startswith("--"):
                assert word in FLAGS, f"compose service {name!r} passes {word}"
    # Without this the loop can match nothing and pass by doing nothing, which
    # is how the first version of this test missed the bug it was written for.
    assert checked >= 2, f"expected several app services, inspected {checked}"


def test_the_service_names_are_the_hostnames_the_config_already_has() -> None:
    """The packaged defaults name minio, iceberg-rest, kafka and
    schema-registry. Naming the Services the same means in-namespace DNS
    resolves them and no configuration changes between Compose and
    Kubernetes."""
    published = {
        d["metadata"]["name"]: {p["port"] for p in d["spec"]["ports"]}
        for d in load("10-stack.yaml")
        if d["kind"] == "Service"
    }
    services = set(published)
    # Both switches on, so both subsystems are configured and comparable.
    app = AppConfig.load(
        env={"ICEBERG_WAREHOUSE": "s3://warehouse/", "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"}
    )
    assert app.catalog is not None and app.kafka is not None
    assert app.kafka.bootstrap_servers.split(":")[0] in services
    for url in (app.catalog.uri, app.catalog.s3_endpoint, app.kafka.schema_registry):
        assert url is not None
        host, _, port = url.split("//", 1)[1].partition(":")
        assert host in services, f"{host} has no Service"
        # A Service that moved its port is as broken as one that was renamed.
        assert int(port) in published[host], f"{host} publishes no port {port}"


# --- things that only fail on a cluster -------------------------------------


def test_every_environment_entry_has_a_name_and_one_source(
    everything: list[dict[str, Any]],
) -> None:
    """A comma in an unquoted flow-mapping value splits the mapping, so
    `{name: X, value: broker,controller}` parses as value "broker" plus a
    stray key. The broker then starts with half its roles and the failure
    arrives as something unrelated."""
    for doc in everything:
        for container in containers(doc):
            for entry in container.get("env", []):
                assert set(entry) <= {"name", "value", "valueFrom"}, entry
                assert "name" in entry, entry
                assert ("value" in entry) ^ ("valueFrom" in entry), entry
                if "value" in entry:
                    assert isinstance(entry["value"], str), entry


def test_no_manifest_contains_a_null(everything: list[dict[str, Any]]) -> None:
    """The same trap, anywhere else. A stray key from a split flow mapping
    carries a null value, and nothing downstream reports it."""

    def walk(node: Any, path: str) -> list[str]:
        if node is None:
            return [path]
        if isinstance(node, dict):
            return [p for k, v in node.items() for p in walk(v, f"{path}.{k}")]
        if isinstance(node, list):
            return [p for i, v in enumerate(node) for p in walk(v, f"{path}[{i}]")]
        return []

    nulls = [p for d in everything for p in walk(d, d["kind"])]
    assert nulls == []


def test_third_party_images_are_pinned_by_digest(everything: list[dict[str, Any]]) -> None:
    """A tag can move. The only unpinned image is the one this repo builds,
    which is side-loaded rather than pulled."""
    for doc in everything:
        for container in containers(doc):
            image = container["image"]
            if image == "pipeline:local":
                continue
            assert "@sha256:" in image, image


def test_the_local_image_is_never_pulled(everything: list[dict[str, Any]]) -> None:
    """It exists in no registry. Always would make the kubelet try."""
    for doc in everything:
        for container in containers(doc):
            if container["image"] == "pipeline:local":
                assert container.get("imagePullPolicy") == "IfNotPresent", doc["metadata"]["name"]


def test_everything_lands_in_one_namespace(everything: list[dict[str, Any]]) -> None:
    for doc in everything:
        if doc["kind"] == "Namespace":
            assert doc["metadata"]["name"] == NAMESPACE
            continue
        assert doc["metadata"].get("namespace") == NAMESPACE, doc["metadata"]["name"]


@pytest.mark.parametrize("fixture", ["stream", "batch"])
def test_the_driver_runs_as_the_account_the_rbac_grants(
    fixture: str, request: pytest.FixtureRequest
) -> None:
    """The driver creates its own executor pods. A default ServiceAccount
    cannot, and the application fails at the first executor request."""
    doc = request.getfixturevalue(fixture)
    spec = doc["spec"].get("template", doc["spec"])
    accounts = {
        d["metadata"]["name"] for d in load("00-namespace.yaml") if d["kind"] == "ServiceAccount"
    }
    assert spec["driver"]["serviceAccount"] in accounts


@pytest.mark.parametrize("fixture", ["stream", "batch"])
def test_every_spark_pod_asserts_it_is_not_root(
    fixture: str, request: pytest.FixtureRequest
) -> None:
    """runAsUser alone is a request. runAsNonRoot is the assertion: an image
    that stopped dropping privileges is refused rather than run as root."""
    doc = request.getfixturevalue(fixture)
    spec = doc["spec"].get("template", doc["spec"])
    for role in ("driver", "executor"):
        context = spec[role]["podSecurityContext"]
        assert context["runAsNonRoot"] is True
        assert context["runAsUser"] == 185


def test_the_streaming_job_does_not_scale_itself(stream: dict[str, Any]) -> None:
    """Executors hold state store partitions. Removing one forces its state to
    be reloaded elsewhere, and a continuous query has no idle period for that
    to be free."""
    assert "dynamicAllocation" not in stream["spec"]
    assert stream["spec"]["executor"]["instances"] >= 1


def test_the_batch_job_scales_and_tracks_shuffle(batch: dict[str, Any]) -> None:
    """Kubernetes has no external shuffle service, so an executor removed
    while another stage still needs its shuffle output forces a recompute."""
    template = batch["spec"]["template"]
    allocation = template["dynamicAllocation"]
    assert allocation["enabled"] is True
    assert allocation["shuffleTrackingEnabled"] is True
    # The operator turns the field above into the --conf flag itself, and
    # defaults it to true. Stating it twice is how the two come to disagree.
    assert "spark.dynamicAllocation.shuffleTracking.enabled" not in template["sparkConf"]


def test_a_batch_run_never_overlaps_the_next(batch: dict[str, Any]) -> None:
    """Two drivers writing the same Iceberg table race for the same commit."""
    assert batch["spec"]["concurrencyPolicy"] == "Forbid"


def test_the_streaming_job_restarts_and_the_batch_job_does_not(
    stream: dict[str, Any], batch: dict[str, Any]
) -> None:
    """A streaming query is meant to be running, so a dead driver is an event
    to recover from. A failed batch run has a next attempt on the schedule,
    and restarting it in place would race that attempt."""
    assert stream["spec"]["restartPolicy"]["type"] == "Always"
    assert batch["spec"]["template"]["restartPolicy"]["type"] == "Never"


def test_spark_conf_values_are_strings(stream: dict[str, Any], batch: dict[str, Any]) -> None:
    """The CRD types sparkConf as map[string]string. A bare true or 2 is a
    boolean or an integer to YAML and the resource is rejected."""
    for spec in (stream["spec"], batch["spec"]["template"]):
        for key, value in spec["sparkConf"].items():
            assert isinstance(value, str), (key, value)
