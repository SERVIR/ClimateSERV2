import React, { Component } from "react";
import { render } from "react-dom";

class App extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
        <div>TEST</div>
    );
  }
}

export default App;

const container = document.getElementById("app");
render(<App />, container);
