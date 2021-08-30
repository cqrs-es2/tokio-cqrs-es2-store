pub static INSERT_EVENT: &str = "
INSERT INTO
    events
    (
        aggregate_type, 
        aggregate_id,
        sequence,
        payload, 
        metadata
    )
VALUES
    (
        ?,
        ?,
        ?,
        ?,
        ?
    );
";

pub static SELECT_EVENTS: &str = "
SELECT
    sequence,
    payload,
    metadata
FROM
    events
WHERE
    aggregate_type = ?
    AND
    aggregate_id = ?
ORDER BY 
    sequence;
";

pub static INSERT_SNAPSHOT: &str = "
INSERT INTO
    snapshots 
    (
        version,
        payload,
        aggregate_type,
        aggregate_id
    )
VALUES
    (
        ?,
        ?,
        ?,
        ?
    );
";

pub static UPDATE_SNAPSHOT: &str = "
UPDATE
    snapshots
SET
    version = ?,
    payload = ?
WHERE
    aggregate_type = ?
    AND
    aggregate_id = ?;
";

pub static SELECT_SNAPSHOT: &str = "
SELECT
    version,
    payload
FROM
    snapshots
WHERE
    aggregate_type = ?
    AND
    aggregate_id = ?;
";

pub static INSERT_QUERY: &str = "
INSERT INTO
    queries 
    (
        version,
        payload,
        aggregate_type,
        aggregate_id,
        query_type
    )
VALUES
    (
        ?,
        ?,
        ?,
        ?,
        ?
    );
";

pub static UPDATE_QUERY: &str = "
UPDATE
    queries
SET
    version = ?,
    payload = ?
WHERE
    aggregate_type = ?
    AND
    aggregate_id = ?
    AND
    query_type = ?;
";

pub static SELECT_QUERY: &str = "
SELECT
    version,
    payload
FROM
    queries
WHERE
    aggregate_type = ?
    AND
    aggregate_id = ?
    AND
    query_type = ?;
";
