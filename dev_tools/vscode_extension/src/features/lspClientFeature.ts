import * as vscode from "vscode";
import { LanguageClient, LanguageClientOptions, ServerOptions } from "vscode-languageclient/node";

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
