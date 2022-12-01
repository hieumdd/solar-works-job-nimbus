import { HttpFunction } from '@google-cloud/functions-framework/build/src/functions';

import { pipelines, pipelineService, taskService } from './job-nimbus/job-nimbus.service';

type Body = {
    pipeline?: string;
    task?: string;
};

export const main: HttpFunction = async (req, res) => {
    const body = req.body as Body;

    console.log('body', JSON.stringify(body));

    if (body.pipeline) {
        pipelineService(pipelines[body.pipeline])
            .then((data) => res.status(200).json({ data }))
            .catch((err) => {
                console.log('error', JSON.stringify(err));
                res.status(500).json({ err });
            });
    } else if (body.task) {
        taskService()
            .then((data) => res.status(200).json({ data }))
            .catch((err) => res.status(500).json({ err }));
    } else {
        res.status(404).end();
    }
};
