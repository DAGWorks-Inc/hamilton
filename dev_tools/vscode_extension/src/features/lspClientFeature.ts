import { exec, ExecException } from "child_process";
import * as vscode from "vscode";
import { LanguageClient, LanguageClientOptions, ServerOptions } from "vscode-languageclient/node";

const checkDependencies = (
  pythonPath: string,
  callback: (error: ExecException | null, packageAvailable: boolean) => void,
) => {
  const command = `${pythonPath} -c "import pkgutil; print(1 if pkgutil.find_loader('hamilton_lsp') else 0)"`;

  exec(command, (error, stdout, stderr) => {
    if (error) {
      callback(error, false);
      return;
    }
    if (stderr) {
      callback(new Error(stderr), false);
      return;
    }

    const packageAvailable = parseInt(stdout.trim()) === 1;
    callback(null, packageAvailable);
  });
};

export const startLspServer = (context: vscode.ExtensionContext, pythonPath: string) => {
  let lspServer: LSPClientFeature | undefined = undefined;

  const message = "Couldn't start Hamilton extension. Check if `sf-hamilton[lsp]` is installed.";
  const action = { title: "Install in current Python env" };
  const installCommand =
    'import sys, runpy; sys.argv = ["pip", "install", ""sf-hamilton[lsp]""]; runpy.run_module("pip", run_name="__main__")';

  checkDependencies(pythonPath, (error, available) => {
    if (error || available == false) {
      vscode.window.showInformationMessage(message, action);
    } else {
      lspServer = new LSPClientFeature(context, pythonPath);
    }
  });

  return lspServer;
};

export class LSPClientFeature implements vscode.Disposable {
  private client: LanguageClient;

  constructor(context: vscode.ExtensionContext, pythonPath: string) {
    const outputChannel = vscode.window.createOutputChannel("Hamilton Language Server", { log: true });

    outputChannel.info("Python interpreter:", `"${pythonPath}"`);
    outputChannel.info("Extension context path:", `"${context.asAbsolutePath("")}"`);

    const serverOptions: ServerOptions = {
      command: pythonPath,
      args: ["-m", "hamilton_lsp"],
      options: { cwd: context.asAbsolutePath("") },
    };

    const clientOptions: LanguageClientOptions = {
      documentSelector: [
        { scheme: "file", language: "python" },
        { scheme: "untitle", language: "python" },
        { scheme: "vscode-notebook", language: "python" },
        { scheme: "vscode-notebook-cell", language: "python" },
      ],
      outputChannel: outputChannel,
      traceOutputChannel: outputChannel,
    };

    this.client = new LanguageClient("hamilton-lsp", "Hamilton Language Client", serverOptions, clientOptions);
    this.client.start();
    this.bindEventListener();

    context.subscriptions.push(
      vscode.window.onDidChangeActiveTextEditor((editor: vscode.TextEditor | undefined) => {
        if (editor && (editor.document.languageId === "python" || editor.document.fileName.endsWith(".py"))) {
          this.client.sendRequest("textDocument/didChange", {
            textDocument: { uri: editor.document.uri.toString(), version: editor.document.version },
            contentChanges: [],
          });
        }
      }),
    );
  }

  private bindEventListener() {
    this.client.onNotification("lsp-view-response", (response) => {
      vscode.commands.executeCommand("hamilton.dataflowWebview.update", response);
    });
  }

  public dispose(): any {
    if (!this.client) {
      undefined;
    }
    this.client.stop();
  }
}
