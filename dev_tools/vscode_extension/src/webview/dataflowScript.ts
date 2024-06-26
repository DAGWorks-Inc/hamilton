import * as d3 from "d3";
import { graphviz } from "d3-graphviz";

const vscode = acquireVsCodeApi();

const scale = 0.8;
function attributer(datum: any, index: any, nodes: any) {
  var selection = d3.select(this);
  if (datum.tag == "svg") {
    datum.attributes = {
      ...datum.attributes,
      width: "100%",
      height: "100%",
    };
    // svg is constructed by hpcc-js/wasm, which uses pt instead of px, so need to convert
    const px2pt = 3 / 4;

    // get graph dimensions in px. These can be grabbed from the viewBox of the svg
    // that hpcc-js/wasm generates
    const graphWidth = datum.attributes.viewBox.split(" ")[2] / px2pt;
    const graphHeight = datum.attributes.viewBox.split(" ")[3] / px2pt;

    // new viewBox width and height
    const w = graphWidth / scale;
    const h = graphHeight / scale;

    // new viewBox origin to keep the graph centered
    const x = -(w - graphWidth) / 2;
    const y = -(h - graphHeight) / 2;

    const viewBox = `${x * px2pt} ${y * px2pt} ${w * px2pt} ${h * px2pt}`;
    selection.attr("viewBox", viewBox);
    datum.attributes.viewBox = viewBox;
  }
}

window.addEventListener("message", (event) => {
  const message = event.data;

  switch (message.command) {
    case "update":
      graphviz("#dataflow").attributer(attributer).renderDot(message.details.dot);
      break;
  }
});
