import React, { Component } from "react";

import { getPipelineRuns } from './services/pipeline_run';

import './app.css';

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      loading: true,
      items: [],
    };
  }

  async componentDidMount() {
    const items = await getPipelineRuns();
    this.setState({
      loading: false,
      items,
    });
  }

  pipelineRunClick = uuid => {
    alert(uuid);
  }

  render() {
    return (
        <div className="container">
          <div className="row">
            <div className="col">
              <h4>Pipeline runs</h4>
              {this.state.loading
                ? <div>Loading...</div>
                : <div className="list-group">
                    {this.state.items
                      ? this.state.items.map((item, index) =>
                        <button className="list-group-item list-group-item-action" key={item.uuid} onClick={() => this.pipelineRunClick(item.uuid)} type="button">
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
}

export default App;
