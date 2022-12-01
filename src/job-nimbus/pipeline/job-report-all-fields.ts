import { Pipeline } from './pipeline.interface';
import { transformDate } from '../pipeline.transform';

export const jobReportAllFields: Pipeline = {
    url: 'https://app.jobnimbus.com/report/e28afb3e19f24e899b7f71ce12a3124e?view=1',
    transformFn: (row) => ({
        name: row['Name'],
        record_type: row['Record Type'],
        status: row['Status'],
        last_phone_call: transformDate(row['Last Phone Call']),
        count: row['Count'],
        id: row['Id'],
        related: row['Related'],
        primary: row['Primary'],
        task_id: row['Task Id'],
        job_id: row['Job Id'],
        contact_id: row['Contact Id'],
    }),
    loadOptions: {
        table: 'CustomerContact',
        schema: [
            { name: 'name', type: 'STRING' },
            { name: 'record_type', type: 'STRING' },
            { name: 'status', type: 'STRING' },
            { name: 'last_phone_call', type: 'TIMESTAMP' },
            { name: 'count', type: 'NUMERIC' },
            { name: 'id', type: 'STRING' },
            { name: 'related', type: 'STRING' },
            { name: 'primary', type: 'STRING' },
            { name: 'task_id', type: 'STRING' },
            { name: 'job_id', type: 'STRING' },
            { name: 'contact_id', type: 'STRING' },
        ],
        writeDisposition: 'WRITE_TRUNCATE',
    },
};
