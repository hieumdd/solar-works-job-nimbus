import { Pipeline } from './pipeline.interface';
import { transformFloat, transformDateTime } from '../pipeline.transform';

export const companyPresentationRan: Pipeline = {
    url: 'https://app.jobnimbus.com/report/c26ab76397a8459da3f5edc1b41c045d?view=1',
    transformFn: (row) => {
        row
        return {
            sales_rep: row['Sales Rep'],
            status: row['Status'],
            total_install_cost: transformFloat(row['Total Install Cost']),
            source: row['Source'],
            stage: row['Stage'],
            date_status_change: transformDateTime(row['Date Status Change']),
            date_created: transformDateTime(row['Date Created']),
            created_by: row['Created By'],
            display: row['Display'],
            state: row['State'],
            id: row['Id'],
            related: row['Related'],
            contact_id: row['Contact Id'],
        };
    },
    loadOptions: {
        table: 'CompanyPresentationRan',
        schema: [
            { name: 'sales_rep', type: 'STRING' },
            { name: 'status', type: 'STRING' },
            { name: 'total_install_cost', type: 'NUMERIC' },
            { name: 'source', type: 'STRING' },
            { name: 'stage', type: 'STRING' },
            { name: 'date_status_change', type: 'DATETIME' },
            { name: 'date_created', type: 'DATETIME' },
            { name: 'created_by', type: 'STRING' },
            { name: 'display', type: 'STRING' },
            { name: 'state', type: 'STRING' },
            { name: 'id', type: 'STRING' },
            { name: 'related', type: 'STRING' },
            { name: 'contact_id', type: 'STRING' },
        ],
        writeDisposition: 'WRITE_TRUNCATE',
    },
};
