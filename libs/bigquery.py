from typing import Callable

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()


def load(dataset: str, table: str, schema: list[dict]) -> Callable[[list[dict]], int]:
    def _load(rows: list[dict]) -> int:
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{dataset}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_TRUNCATE",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        # _update(dataset, table)

        return output_rows

    return _load


def _update(dataset: str, table: str) -> None:
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {dataset}.{table} AS
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, account ORDER BY _batched_at DESC)
            AS row_num
        FROM {dataset}._stage_{table}
    ) WHERE row_num = 1"""
    ).result()
