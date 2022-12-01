import { Pipeline } from './pipeline/pipeline.interface';
import { companyPresentationRan } from './pipeline/company-presentation-ran';
import { companyTotalDeals } from './pipeline/company-total-deals';
import { completedInstalls } from './pipeline/completed-installs';
import { contactReportsAllFields } from './pipeline/contact-reports-all-fields';
import { customerContact } from './pipeline/customer-contact';
import { jobReportAllFields } from './pipeline/job-report-all-fields';
import { leadsOverall } from './pipeline/leads-overall';
import { sales } from './pipeline/sales';
import { scrapeService } from './scrape.service';
import { load } from '../bigquery/bigquery.service';
import { transformNull } from './pipeline.transform';
import { createTasks } from '../tasks/cloud-tasks.service';

export const pipelines = Object.fromEntries(
    [
        companyPresentationRan,
        companyTotalDeals,
        completedInstalls,
        contactReportsAllFields,
        customerContact,
        jobReportAllFields,

        // !TODO
        leadsOverall,

        sales,
    ].map((pipeline) => [pipeline.loadOptions.table, pipeline]),
);

export const pipelineService = async (pipeline: Pipeline) => {
    const scrapedData = await scrapeService(pipeline.url);

    if (!scrapedData) throw new Error('no data');

    const data = scrapedData
        .map((row) =>
            Object.fromEntries(
                Object.entries(row).map(([key, value]) => [
                    key.trim().replace(':', ''),
                    transformNull(value),
                ]),
            ),
        )
        .map(pipeline.transformFn);

    return load(data, pipeline.loadOptions);
};

export const taskService = () => {
    return createTasks(
        Object.values(pipelines).map((pipeline) => ({ pipeline: pipeline.loadOptions.table })),
        (pipeline) => pipeline.pipeline,
    );
};
