import * as vscode from "vscode";
import { DataflowWebviewFeature } from "./features/dataflowWebviewFeature";
import { LSPClientFeature, startLspServer } from "./features/lspClientFeature";
import { SupportLinksFeature } from "./features/supportLinksFeature";

let extensionFeatures: any[];

export async function activate(context: vscode.ExtensionContext) {
  const pythonExtension = vscode.extensions.getExtension("ms-python.python");
  const pythonPath = pythonExtension?.exports.settings.getExecutionDetails().execCommand?.join("");

  const lspServer: LSPClientFeature | undefined = startLspServer(context, pythonPath);

  extensionFeatures = [lspServer, new DataflowWebviewFeature(context), new SupportLinksFeature()];
}

export function deactivate() {
  extensionFeatures.forEach((feature) => feature.dispose());
}
