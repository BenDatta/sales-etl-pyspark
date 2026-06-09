import os
from pathlib import Path


def get_spark_session(app_name="sales-etl-pyspark"):
    """Configure Java/logging env vars and return a quiet SparkSession."""
    os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    log4j_url = (Path(__file__).parent / "log4j2-quiet.properties").resolve().as_uri()
    os.environ["SPARK_SUBMIT_OPTS"] = f"-Dlog4j2.configurationFile={log4j_url}"
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--conf spark.log.level=ERROR "
        f"--driver-java-options -Dlog4j2.configurationFile={log4j_url} "
        "pyspark-shell"
    )

    from pyspark.sql import SparkSession

    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    stderr_fd = os.dup(2)
    os.dup2(devnull_fd, 2)
    try:
        return (
            SparkSession.builder.appName(app_name)
            .config("spark.log.level", "ERROR")
            .config(
                "spark.driver.extraJavaOptions",
                f"-Dlog4j2.configurationFile={log4j_url}",
            )
            .getOrCreate()
        )
    finally:
        os.dup2(stderr_fd, 2)
        os.close(devnull_fd)
        os.close(stderr_fd)
