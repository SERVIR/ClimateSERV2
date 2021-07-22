import config from '../config.json';

export async function getPipelineRuns() {
    let url = `${config.API_URL}/api/etl_pipeline_run/?format=json`;
    const response = await fetch(url);
    return response ? response.json() : null;
}
