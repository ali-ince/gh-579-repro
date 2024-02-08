import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.readwriter import DataStreamWriter


def read_stream(
    spark: SparkSession,
    streaming_from: str,
    url: str,
    username: str | None,
    password: str | None,
):
    stream = (
        spark.readStream.format("org.neo4j.spark.DataSource")
        .option("url", url)
        .option("streaming.from", streaming_from)
        .option("streaming.property.name", "timestamp")
        .option("query", """
            MATCH (p:Test)
            WHERE p.timestamp > $stream.offset
            RETURN p.age AS age, p.timestamp AS timestamp""")
        .option("streaming.query.offset", "MATCH (p:Test) RETURN max(p.timestamp)")
    )
    if username:
        stream = stream.option("authentication.basic.username", username)
    if password:
        stream = stream.option("authentication.basic.password", password)

    return stream.load()


def stream_writer(stream_df: DataFrame, checkpoint_location: str):
    return stream_df.writeStream.option(
        "checkpointLocation", checkpoint_location
    ).format("console")


def repro(writer: DataStreamWriter, processing_time: str = "500 milliseconds"):
    print("RUN 1")
    streaming_query = writer.trigger(processingTime=processing_time).start()
    streaming_query.awaitTermination(3)
    streaming_query.stop()

    # Stop the stream and resume for one trigger simulating failure after restart.
    print("RUN 2")
    streaming_query = writer.trigger(once=True).start()
    streaming_query.awaitTermination(3)
    streaming_query.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address", type=str, default="bolt://localhost:7687")
    parser.add_argument("-p", "--password", type=str, nargs="?")
    parser.add_argument("-u", "--username", type=str, nargs="?")
    parser.add_argument("-c", "--checkpointLocation", type=str, required=True)
    parser.add_argument(
        "-f", "--streamingFrom", type=str, choices=["NOW", "ALL"], default="ALL"
    )
    parser.add_argument(
        "-l", "--logLevel", type=str, choices=["FATAL", "ERROR", "INFO", "DEBUG", "TRACE"], default="FATAL"
    )
    args = parser.parse_args()
    spark = (
        SparkSession.Builder()
        .config(
            "spark.jars",
            "jars/neo4j.jar"
        )
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.logLevel)

    stream_df = read_stream(
        spark, args.streamingFrom, args.address, args.username, args.password
    )
    writer = stream_writer(stream_df, args.checkpointLocation)
    repro(writer)
