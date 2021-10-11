import config from '../config.json';

export async function getPipelineRuns() {
    let url = `${config.API_URL}/api/etl_pipeline_run/?format=json`;
    return fetch(url);
}
