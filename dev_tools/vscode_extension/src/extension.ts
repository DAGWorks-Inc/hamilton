import * as vscode from "vscode";
import { execSync } from "child_process";
import { DataflowWebviewFeature } from "./features/dataflowWebviewFeature";
import { LSPClientFeature } from "./features/lspClientFeature";
import { SupportLinksFeature } from "./features/supportLinksFeature";


const checkPythonDependencies = (pythonPath: string) => {
  try{
    execSync(`${pythonPath} -c "import hamilton_lsp"`)
    return true
  } catch (error) {
    return false
  }
}

function installDependenciesMessage(pythonPath: string) {
  let installLSP = "Install sf-hamilton[lsp]"

  vscode.window.showInformationMessage("Missing the Hamilton language server.", installLSP)
    .then(selection => {
      if (selection === installLSP) {
        execSync(`${pythonPath} -m pip install 'sf-hamilton-lsp'`)
        vscode.commands.executeCommand("workbench.action.reloadWindow")
      }
    })
}



let extensionFeatures: any[];

export async function activate(context: vscode.ExtensionContext) {
  const pythonExtension = vscode.extensions.getExtension("ms-python.python");
  const pythonPath = pythonExtension?.exports.settings.getExecutionDetails().execCommand?.join("");

  const available = checkPythonDependencies(pythonPath)
  if (available === false) {
    installDependenciesMessage(pythonPath)
    return
  }

  extensionFeatures = [
    new LSPClientFeature(context, pythonPath),
    new DataflowWebviewFeature(context),
    new SupportLinksFeature()
  ];
}

export function deactivate() {
  extensionFeatures.forEach((feature) => feature.dispose());
}
