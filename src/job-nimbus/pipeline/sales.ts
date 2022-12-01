import { Pipeline } from './pipeline.interface';
import { transformFloat, transformDateTime } from '../pipeline.transform';

export const sales: Pipeline = {
    url: 'https://app.jobnimbus.com/report/b1828bca05704fda8266e1456cd7f679?view=1',
    transformFn: (row) => ({
        display: row['Display'],
        date_status_change: transformDateTime(row['Date Status Change']),
        status: row['Status'],
        source: row['Source'],
        sales_rep: row['Sales Rep'],
        state: row['State'],
        stage: row['Stage'],
        address_info: row['Address Info'],
        lender: row['Lender'],
        interestrate: transformFloat(row['Interestrate']),
        years: transformFloat(row['Years']),
        total_install_cost: transformFloat(row['Total Install Cost']),
        numberofmodules: transformFloat(row['Numberofmodules']),
        location: row['Location'],
        panel_type: row['Panel Type'],
        id: row['Id'],
        related: row['Related'],
        contact_id: row['Contact Id'],
    }),
    loadOptions: {
        table: 'Sales',
        schema: [
            { name: 'display', type: 'STRING' },
            { name: 'date_status_change', type: 'TIMESTAMP' },
            { name: 'status', type: 'STRING' },
            { name: 'source', type: 'STRING' },
            { name: 'sales_rep', type: 'STRING' },
            { name: 'state', type: 'STRING' },
            { name: 'stage', type: 'STRING' },
            { name: 'address_info', type: 'STRING' },
            { name: 'lender', type: 'STRING' },
            { name: 'interestrate', type: 'NUMERIC' },
            { name: 'years', type: 'NUMERIC' },
            { name: 'total_install_cost', type: 'NUMERIC' },
            { name: 'numberofmodules', type: 'NUMERIC' },
            { name: 'location', type: 'STRING' },
            { name: 'panel_type', type: 'STRING' },
            { name: 'id', type: 'STRING' },
            { name: 'related', type: 'STRING' },
            { name: 'contact_id', type: 'STRING' },
        ],
        writeDisposition: 'WRITE_TRUNCATE',
    },
};