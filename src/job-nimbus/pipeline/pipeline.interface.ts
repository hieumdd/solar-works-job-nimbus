import { LoadOptions } from '../../bigquery/bigquery.service';

export type Pipeline = {
    url: string;
    transformFn: (e: Record<string, any>) => Record<string, any>;
    loadOptions: LoadOptions;
};
