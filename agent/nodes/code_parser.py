"""AST-based code parser node for PySpark and Prefect jobs."""

import ast
import re
from typing import Any

import structlog

from agent.state import AgentState

logger = structlog.get_logger(__name__)

_SPARK_TRANSFORMS = {
    "select", "filter", "where", "groupBy", "groupby",
    "agg", "join", "union", "unionAll", "distinct",
    "orderBy", "sort", "limit", "drop", "withColumn",
    "withColumnRenamed", "dropDuplicates", "fillna",
    "dropna", "cache", "persist", "repartition", "coalesce",
    "write", "read", "load", "save", "saveAsTable",
    "insertInto", "createOrReplaceTempView",
}

_PREFECT_DECORATORS = {"flow", "task"}

_TYPE_MAP = {
    "str": "StringType",
    "int": "IntegerType",
    "float": "FloatType",
    "bool": "BooleanType",
    "date": "DateType",
    "datetime": "TimestampType",
    "bytes": "BinaryType",
}


def _extract_imports(tree: ast.Module) -> list[str]:
    """Return list of top-level imported module names."""
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module.split(".")[0])
    return sorted(set(imports))


def _extract_functions(tree: ast.Module) -> list[dict[str, Any]]:
    """Extract top-level function definitions with decorator info."""
    funcs = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            decorators = []
            for dec in node.decorator_list:
                if isinstance(dec, ast.Name):
                    decorators.append(dec.id)
                elif isinstance(dec, ast.Attribute):
                    decorators.append(dec.attr)
            funcs.append(
                {
                    "name": node.name,
                    "decorators": decorators,
                    "lineno": node.lineno,
                    "args": [
                        a.arg for a in node.args.args
                    ],
                }
            )
    return funcs


def _extract_transforms(source: str) -> list[str]:
    """Find Spark DataFrame transform method calls via regex."""
    found = []
    for t in _SPARK_TRANSFORMS:
        if re.search(rf"\.{t}\s*\(", source):
            found.append(t)
    return sorted(found)


def _extract_column_names(source: str) -> list[str]:
    """Extract string literals that look like column names."""
    cols: list[str] = []
    for match in re.finditer(
        r'(?:col\(|F\.col\(|[\'"]([\w_]+)[\'"])', source
    ):
        name = match.group(1)
        if name and len(name) > 1 and not name.startswith("_"):
            cols.append(name)
    return sorted(set(cols))


def _detect_input_type(
    source: str, functions: list[dict]
) -> str:
    """Detect whether source is pyspark or prefect."""
    decorators = {
        d
        for f in functions
        for d in f.get("decorators", [])
    }
    if decorators & _PREFECT_DECORATORS:
        return "prefect"
    if "pyspark" in source or "SparkSession" in source:
        return "pyspark"
    return "pyspark"


def parse_job(state: AgentState) -> AgentState:
    """Parse input code and populate parsed_structure in state."""
    source = state["input_code"]
    session_id = state["session_id"]
    logger.info("parse_job_start", session_id=session_id)

    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        logger.error(
            "parse_job_syntax_error",
            session_id=session_id,
            error=str(exc),
        )
        return {
            **state,
            "error": f"E001: Syntax error in input: {exc}",
            "parsed_structure": {},
        }

    imports = _extract_imports(tree)
    functions = _extract_functions(tree)
    transforms = _extract_transforms(source)
    columns = _extract_column_names(source)
    detected_type = _detect_input_type(source, functions)

    line_count = len(source.splitlines())
    has_spark_session = "SparkSession" in source
    has_schema_def = (
        "StructType" in source or "schema=" in source
    )
    has_write = any(
        t in transforms for t in ["write", "save", "saveAsTable"]
    )

    parsed = {
        "imports": imports,
        "functions": functions,
        "transforms": transforms,
        "columns": columns,
        "detected_type": detected_type,
        "line_count": line_count,
        "has_spark_session": has_spark_session,
        "has_schema_def": has_schema_def,
        "has_write": has_write,
        "prefect_tasks": [
            f["name"]
            for f in functions
            if "task" in f.get("decorators", [])
        ],
        "prefect_flows": [
            f["name"]
            for f in functions
            if "flow" in f.get("decorators", [])
        ],
    }

    logger.info(
        "parse_job_complete",
        session_id=session_id,
        transforms=transforms,
        columns_found=len(columns),
        input_type=detected_type,
    )

    return {
        **state,
        "parsed_structure": parsed,
        "input_type": detected_type,
        "error": None,
    }
