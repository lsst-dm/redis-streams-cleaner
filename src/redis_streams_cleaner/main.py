import logging
import os
import sys
import time

import redis

from .usdf_json_formatter import UsdfJsonFormatter

log_handler = logging.StreamHandler(stream=sys.stdout)
log_handler.setFormatter(UsdfJsonFormatter())
if os.environ.get("DEBUG_LOGS") == "true":
    log_level = logging.DEBUG
else:
    log_level = logging.INFO

logging.basicConfig(handlers=[log_handler], level=log_level)
logger = logging.getLogger(__name__)


def process_streams(
    client: redis.Redis,
    stream_groups: list[tuple[str, str]],
    idle_threshold: int,
    max_retries: int,
) -> tuple[int, int, list[tuple[str, str, str]]]:
    """Process all configured streams for idle consumer cleanup.

    Parameters
    ----------
    client : redis.Redis
        Connected Redis client.
    stream_groups : list[tuple[str, str]]
        List of (stream_name, group_name) tuples.
    idle_threshold : int
        Idle time threshold in milliseconds.

    Returns
    -------
    tuple[int, int, list[tuple[str, str, str]]]
        (total_removed, total_failed, failed_streams)
        where failed_streams is list of (stream, group, error) tuples.
    """
    total_removed = 0
    total_failed = 0
    failed_streams = []

    for stream_name, group_name in stream_groups:
        try:
            logger.info(f"Processing: {stream_name}/{group_name}")
            removed, failed = cleanup_idle_consumers(
                client, stream_name, group_name, idle_threshold, max_retries
            )
            total_removed += removed
            total_failed += failed
        except redis.ConnectionError as e:
            logger.exception(
                f"Connection error for {stream_name}/{group_name}"
            )
            failed_streams.append((stream_name, group_name, str(e)))
        except redis.ResponseError:
            logger.exception(f"Redis error for {stream_name}/{group_name}")
            failed_streams.append((stream_name, group_name))
        except Exception:
            logger.exception(f"Error processing {stream_name}/{group_name}")
            failed_streams.append((stream_name, group_name))

    return total_removed, total_failed, failed_streams


def cleanup_idle_consumers(
    client: redis.Redis,
    stream_name: str,
    group_name: str,
    idle_threshold_ms: int,
    max_retries: int,
) -> tuple[int, int]:
    """Remove idle consumers from a Redis Streams consumer group.

    Identifies consumers in the specified stream/group that have been idle
    longer than the threshold, then attempts to remove each idle consumer.
    Returns counts of successful removals and failures.

    Parameters
    ----------
    client : `redis.Redis`
        Connected Redis client instance.
    stream_name : `str`
        Name of the Redis Stream (may contain colons, e.g.,
        'instrument:lsstcam').
    group_name : `str`
        Name of the consumer group within the stream.

    idle_threshold_ms: `int`
        Idle time threshold in milliseconds. Consumers idle longer than this
        value will be targeted for removal. Must be >= 0.
    max_retries : `int`
        Number of times to retry the redis connection

    Returns
    -------
    tuple[int, int]
        A tuple of (removed_count, failed_count) where:
        - removed_count : int
            Number of consumers successfully removed.
        - failed_count : int
            Number of consumers that failed to remove.

    Raises
    ------
    redis.ResponseError
        If the stream or consumer group does not exist, or if Redis returns
        an error during xinfo_consumers() call.
    Exception
        If an unexpected error occurs while fetching consumer info or during
        removal operations.
    """
    logger.info(
        f"Starting cleanup for {stream_name}/{group_name} "
        f"with idle threshold: {idle_threshold_ms}ms"
    )

    try:
        consumers = client.xinfo_consumers(stream_name, group_name)
        logger.info(f"Found {len(consumers)} consumers")

        removed_count = 0
        failed_count = 0

        for consumer in consumers:
            consumer_name = consumer["name"]
            idle_ms = consumer["idle"]
            pending = consumer["pending"]

            if idle_ms > idle_threshold_ms:
                logger.debug(
                    f"Idle consumer detected: {consumer_name} "
                    f"(idle: {idle_ms}ms, pending: {pending})"
                )

                success = remove_consumer(
                    client, stream_name, group_name, consumer_name, max_retries
                )
                if success:
                    removed_count += 1
                else:
                    failed_count += 1
            else:
                logger.debug(
                    f"{consumer_name}: idle {idle_ms}ms (below threshold)"
                )

        logger.info(
            f"Cleanup complete: {removed_count} removed,{failed_count} failed"
        )

    except redis.ResponseError:
        logger.exception("Redis error")
        raise
    except Exception:
        logger.exception("Unexpected error")
        raise
    else:
        return removed_count, failed_count


def remove_consumer(
    client: redis.Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    max_retries: int,
) -> bool:
    """Remove consumer with retry logic.

    Parameters
    ----------
    client : `redis.Redis`
        Connected Redis client instance.
    stream_name : `str`
        Name of the Redis Stream (may contain colons,
        e.g., 'instrument:lsstcam').
    group_name : `str`
        Name of the consumer group within the stream.
    consumer_name : `str`
        Name of the consumer to remove.
    max_retries : `int`
        Maximum number of retry attempts on connection errors.
        Must be >= 1. Each retry waits 2 seconds before attempting again.

    Returns
    -------
    bool
        `True` if the consumer was successfully removed.
        `False` if the consumer could not be removed due to:
        - Redis response error (e.g., consumer or group does not exist)
        - Unexpected exception during removal
        - All connection retries exhausted (reached max_retries)
    """
    for attempt in range(max_retries):
        try:
            result = client.xgroup_delconsumer(
                stream_name, group_name, consumer_name
            )
            logger.debug(
                f"Removed {consumer_name} (pending messages cleared: {result})"
            )

        except redis.ConnectionError as e:
            logger.warning(
                f"Connection error on attempt {attempt + 1}/{max_retries}: "
                f"{e!s}"
            )
            if attempt < max_retries - 1:
                time.sleep(2)

        except redis.ResponseError:
            logger.exception(f"Redis error removing {consumer_name}")
            return False

        except Exception:
            logger.exception(f"Unexpected error removing {consumer_name}")
            return False
        else:
            return True

    logger.error(
        f"Failed to remove {consumer_name} after {max_retries} attempts"
    )
    return False


def report_job_summary(
    stream_count: int,
    total_removed: int,
    total_failed: int,
    failed_streams: list[tuple[str, str, str]],
) -> None:
    """Log job summary and report any failures.

    Parameters
    ----------
    stream_count : int
        Total number of streams processed.
    total_removed : int
        Total idle consumers removed.
    total_failed : int
        Total failed removal attempts.
    failed_streams : list[tuple[str, str, str]]
        List of (stream, group, error) tuples for failed streams.

    Raises
    ------
    SystemExit
        Exit code 1 if any streams failed, otherwise 0.
    """
    logger.info(
        f"Job Summary: "
        f"Streams processed: {stream_count} | "
        f"Total removed: {total_removed} | "
        f"Total failed: {total_failed}"
    )

    if failed_streams:
        logger.error(f"Failed streams: {len(failed_streams)}")
        sys.exit(1)
    else:
        logger.info("All streams processed successfully")
        sys.exit(0)


def create_redis_client(redis_host: str, redis_port: int) -> redis.Redis:
    """Create and validate Redis connection.

    Parameters
    ----------
    redis_host : str
        Redis server hostname.
    redis_port : int
        Redis server port.

    Returns
    -------
    redis.Redis
        Connected Redis client.

    Raises
    ------
    SystemExit
        If connection to Redis fails.
    """
    try:
        client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        client.ping()
    except Exception:
        logger.exception("Failed to connect to Redis")
        sys.exit(1)
    else:
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        return client


def load_configuration() -> tuple[str, int, str, int]:
    """Load configuration from environment variables.

    Returns
    -------
    tuple[str, int, str, int]
        (redis_host, redis_port, streams_config, idle_threshold)

    Raises
    ------
    SystemExit
        If STREAMS environment variable is not set.
    """
    redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    streams_config = os.getenv(
        "STREAMS", "instrument:lsstcam=group1|instrument:latiss=group2"
    )
    idle_threshold = int(os.getenv("IDLE_THRESHOLD_MS", "60000"))
    max_retries = int(os.getenv("MAX_RETRIES", "3"))

    if not streams_config:
        logger.error("STREAMS environment variable is required")
        logger.error(
            'Format: "instrument:lsstcam=group|instrument:latiss=group"'
        )
        sys.exit(1)

    return redis_host, redis_port, streams_config, idle_threshold, max_retries


def parse_streams_config(streams_str: str) -> list[tuple[str, str]]:
    """Parse stream:group pairs from string.

    Parses a pipe-separated string of stream and consumer group pairs.
    Stream names may contain colons (e.g., 'instrument:lsstcam').

    Format: "instrument:lsstcam=processors|other:stream=loggers"
    - Use `=` to separate stream name from group name
    - Use `|` to separate different stream:group pairs

    Parameters
    ----------
    streams_str : `str`
        Pipe-separated stream=group pairs. Returns empty list if None
        or empty string.

    Returns
    -------
    list[tuple[str, str]]
        List of (stream_name, group_name) tuples.
        Example: [('instrument:lsstcam', 'group'), ('other:stream', 'group')]
        Returns empty list if no valid pairs are found.
    """
    if not streams_str:
        return []

    pairs = []
    for pair_str in streams_str.split("|"):
        pair = pair_str.strip()
        if "=" not in pair:
            logger.warning(f"Invalid stream:group pair: {pair}")
            continue

        stream, group = pair.split("=", 1)
        pairs.append((stream.strip(), group.strip()))

    return pairs


def main() -> None:
    """Execute Redis idle consumer cleanup job.

    Main entry point for the CronJob. Loads configuration from environment
    variables, connects to Redis, and removes idle consumers from specified
    streams and consumer groups.

    Configuration is read from the following environment variables:

    - REDIS_HOST : str, optional
        Redis server hostname (default: '127.0.0.1')
    - REDIS_PORT : int, optional
        Redis server port (default: 6379)
    - STREAMS : str, required
        Pipe-separated stream:consumer group pairs.
        Format: "instrument:lsstcam=group|other:stream=loggers"
    - IDLE_THRESHOLD_MS : int, optional
        Idle time threshold in milliseconds (default: 60000)
    - DEBUG_LOGS : str, optional
        Enable debug logging if set to 'true' (default: 'false')

    Returns
    -------
    None
        Exits with status code 0 on success or 1 on failure.

    Raises
    ------
    SystemExit
        Exit status 0 if all streams processed successfully.
        Exit status 1 if STREAMS is not configured, parsing fails,
        or any stream processing encounters an error.
    """
    logger.info("Redis Streams Cleaner CronJob Starting")

    # Load configuration
    (redis_host, redis_port, streams_config, idle_threshold, max_retries) = (
        load_configuration()
    )

    logger.info(f"Streams config: {streams_config}")

    # Parse streams
    stream_groups = parse_streams_config(streams_config)
    if not stream_groups:
        logger.error("No valid stream=group pairs found in STREAMS")
        sys.exit(1)

    logger.info(f"Processing {len(stream_groups)} stream(s)")
    logger.info(f"Stream Groups {stream_groups}")

    # Connect to Redis
    client = create_redis_client(redis_host, redis_port)

    try:
        # Process streams
        total_removed, total_failed, failed_streams = process_streams(
            client, stream_groups, idle_threshold, max_retries
        )

        # Report results
        report_job_summary(
            len(stream_groups), total_removed, total_failed, failed_streams
        )

    finally:
        try:
            client.close()
            logger.info("Redis connection closed")
        except Exception:
            logger.exception("Error closing connection")


if __name__ == "__main__":
    main()
