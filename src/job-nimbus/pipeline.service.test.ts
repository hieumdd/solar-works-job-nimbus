import { pipelines, pipelineService } from './job-nimbus.service';
import { Pipeline } from './pipeline/pipeline.interface';

describe('Pipeline Service', () => {
    it.each(
        Object.values(pipelines).map((pipeline) => [pipeline.loadOptions.table, pipeline]) as [
            string,
            Pipeline,
        ][],
    )('%p', async (name, pipeline) => {
        console.log(name);
        return pipelineService(pipeline).then((res) => expect(res).toBeGreaterThan(0));
    });
});
