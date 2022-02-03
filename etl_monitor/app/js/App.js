import React, { useEffect, useState } from 'react';
import ReactDOM from "react-dom";
import 'regenerator-runtime/runtime';
import { css } from "@emotion/react";
import FadeLoader from "react-spinners/FadeLoader";

import { getPipelineRuns } from './services/pipeline_run';
import 'bootstrap/dist/css/bootstrap.min.css';
import '../css/app.css';

const override = css`
  display: block;
  margin: 0 auto;
`;

export const App = () => {

  const [pipelineRunRequest, setPipelineRunRequest] = useState({loading: true, pipelineRuns: null});

  useEffect(async () => {
    const fetchData = async () => {
      await getPipelineRuns()
        .then(response => response.ok ? response.json() : Promise.reject())
        .then(data => {
          setPipelineRunRequest({
            loading: false,
            pipelineRuns: data
          });
        })
        .catch(error => {
          setPipelineRunRequest({
            loading: false,
            pipelineRuns: null
          });
        });
    }
    fetchData();
  }, []);

  const pipelineRunClick = uuid => {
    alert(uuid);
  };

  const { loading, pipelineRuns } = pipelineRunRequest;

  return (
      <div className="container">
        <div className="row">
          <div className="col">
            <h4>Pipeline runs</h4>
            {loading ?
              <FadeLoader color="#2C5E7B" loading={loading} css={override} size={150} />
            : <div className="list-group">
              {pipelineRuns ?
                pipelineRuns.map((item, index) =>
                  <button className="list-group-item list-group-item-action" key={item.uuid} onClick={() => pipelineRunClick(item.uuid)} type="button">
                    {item.uuid}
                  </button>)
              : null
              }
              </div>
            }
          </div>
          <div className="col">
            <h4>Logs</h4>
            <h4>Granules</h4>
          </div>
        </div>
      </div>
  );

}

ReactDOM.render(<App />, document.getElementById('app'));
