import { Readable } from 'stream';
import { pipeline } from 'stream/promises';

import ndjson from 'ndjson';
import { BigQuery } from '@google-cloud/bigquery';
import dayjs from 'dayjs';

const client = new BigQuery();

const DATASET = 'JobNimbus';

type WithWriteAppendOptions = {
    rows: Record<string, any>[];
    table: string;
    schema: Record<string, any>[];
};

const withWriteAppend = (options: WithWriteAppendOptions) => {
    const { rows, table, schema } = options;

    return [
        rows.map((row) => ({ ...row, _batched_at: dayjs().toISOString() })),
        `append_${table}`,
        [...schema, { name: '_batched_at', type: 'TIMESTAMP' }],
    ] as [Record<string, any>[], string, Record<string, any>[]];
};

export type LoadOptions = {
    table: string;
    schema: Record<string, any>[];
    writeDisposition: 'WRITE_APPEND' | 'WRITE_TRUNCATE';
};

export const load = async (rows: Record<string, any>[], options: LoadOptions) => {
    const { table, schema, writeDisposition } = options;

    const x = rows.filter((row) => row.last_phone_call === '2022-09-19');

    const [_rows, _table, fields] =
        writeDisposition === 'WRITE_APPEND'
            ? withWriteAppend({ rows, table, schema })
            : [rows, table, schema];

    const tableWriteStream = client.dataset(DATASET).table(_table).createWriteStream({
        schema: { fields },
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        createDisposition: 'CREATE_IF_NEEDED',
        writeDisposition,
    });

    return pipeline(Readable.from(_rows), ndjson.stringify(), tableWriteStream)
        .then(() => rows.length)
        .catch((err) => {
            console.log(err);
        });
};
