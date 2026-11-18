"""PySpark job: run Iceberg maintenance statements for one table.

Executed by the Spark driver from the ConfigMap mount. Self-contained: it must not
import the lake package.

Arguments: <catalog> <namespace.table> <statements as a JSON list>
Environment: CLIENT_ID and CLIENT_SECRET (Polaris OAuth client).
"""

import json
import os
import sys

from pyspark.sql import SparkSession


def main(argv: list[str]) -> int:
    catalog, table, statements_json = argv
    statements = json.loads(statements_json)
    credential = f"{os.environ['CLIENT_ID']}:{os.environ['CLIENT_SECRET']}"
    spark = (
        SparkSession.builder.appName(f"iceberg-maintenance-{table}")
        .config(f"spark.sql.catalog.{catalog}.credential", credential)
        .getOrCreate()
    )
    if not spark.catalog.tableExists(f"{catalog}.{table}"):
        print(f"maintenance skipped: table {table} does not exist yet")
        return 0
    for statement in statements:
        print(f"running: {statement}", flush=True)
        try:
            spark.sql(statement).collect()
        except Exception:
            print(f"failed: {statement}", file=sys.stderr, flush=True)
            raise
    print(f"maintenance finished: {table}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
