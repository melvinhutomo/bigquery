from google.cloud import bigquery
from datetime import timedelta
import pandas as pd

client = bigquery.Client()

sql = """
    MERGE
    INTO `test-melvin-329603.samples.user_data`
    USING (
        SELECT `test-melvin-329603.samples.user_data_update`.cif_id AS join_key,
        `test-melvin-329603.samples.user_data_update`.*
    FROM `test-melvin-329603.samples.user_data_update`
    UNION ALL
    SELECT NULL, `test-melvin-329603.samples.user_data_update`.*
    FROM `test-melvin-329603.samples.user_data_update`
    JOIN `test-melvin-329603.samples.user_data`
    ON `test-melvin-329603.samples.user_data_update`.cif_id = `test-melvin-329603.samples.user_data`.cif_id
    WHERE (`test-melvin-329603.samples.user_data_update`.point <> `test-melvin-329603.samples.user_data`.point
    AND `test-melvin-329603.samples.user_data`.expired_date = '0001-01-01')) sub1
    ON sub1.join_key = `test-melvin-329603.samples.user_data`.cif_id
    WHEN matched
    AND sub1.point <> `test-melvin-329603.samples.user_data`.point THEN UPDATE
    set expired_date = CURRENT_DATE()
    WHEN NOT matched THEN INSERT
    (
        cif_id,
        name,
        point,
        modified_date,
        expired_date
    )
    VALUES
    (
        sub1.cif_id,
        sub1.name,
        sub1.point,
        CURRENT_DATE(),
        '0001-01-01'
    );
"""

df = client.query(sql).to_dataframe()
print(df)