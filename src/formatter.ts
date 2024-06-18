import { Cell, CodeCell } from '@jupyterlab/cells';
import { INotebookTracker, Notebook } from '@jupyterlab/notebook';
import { IEditorTracker } from '@jupyterlab/fileeditor';
import { showErrorMessage } from '@jupyterlab/apputils';
import { format } from 'sql-formatter';
import { RegExpForeignCodeExtractor } from '@jupyter-lsp/jupyterlab-lsp';
import { Constants } from './constants';
import { JupyterFrontEnd } from '@jupyterlab/application';
import { cellMagicExtractor, markerExtractor } from './utils';
import { KeywordCase } from 'sql-formatter';

export class SqlFormatter {
  private formatTabWidth: number;
  private formatUseTabs: boolean;
  private formatKeywordCase: KeywordCase;

  constructor(
    formatTabWidth: number,
    formatUseTabs: boolean,
    formatKeywordCase: KeywordCase
  ) {
    this.formatTabWidth = formatTabWidth;
    this.formatUseTabs = formatUseTabs;
    this.formatKeywordCase = formatKeywordCase;
  }

  format(text: string | null): string {
    const formatted = format(text || '', {
      language: 'spark', // Defaults to "sql" (see the above list of supported dialects)
      tabWidth: this.formatTabWidth, // Defaults to two spaces. Ignored if useTabs is true
      useTabs: this.formatUseTabs, // Defaults to false
      keywordCase: this.formatKeywordCase, // Defaults to false (not safe to use when SQL dialect has case-sensitive identifiers)
      linesBetweenQueries: 2 // Defaults to 1
    });
    return formatted;
  }
}

class JupyterlabNotebookCodeFormatter {
  private notebookTracker: INotebookTracker;
  private working: boolean;
  private extractors: RegExpForeignCodeExtractor[];
  private sqlFormatter: SqlFormatter;
  constructor(notebookTracker: INotebookTracker, sqlFormatter: SqlFormatter) {
    this.working = false;
    this.notebookTracker = notebookTracker;
    this.extractors = [];
    this.extractors.push(cellMagicExtractor('sparksql'));
    this.extractors.push(cellMagicExtractor('trino'));
    this.sqlFormatter = sqlFormatter;
  }

  setFormatter(sqlFormatter: SqlFormatter) {
    this.sqlFormatter = sqlFormatter;
  }

  pushExtractors(
    sparksqlStartMarker: string,
    sparksqlEndMarker: string,
    trinoStartMarker: string,
    trinoEndMarker: string
  ) {
    this.extractors.push(
      markerExtractor(sparksqlStartMarker, sparksqlEndMarker, 'sparksql')
    );
    this.extractors.push(
      markerExtractor(trinoStartMarker, trinoEndMarker, 'trino')
    );
  }

  public async formatAction() {
    return this.formatCells(true);
  }

  public async formatSelectedCodeCells(notebook?: Notebook) {
    return this.formatCells(true, notebook);
  }

  private getCodeCells(selectedOnly = true, notebook?: Notebook): CodeCell[] {
    if (!this.notebookTracker?.currentWidget) {
      return [];
    }
    const codeCells: CodeCell[] = [];
    notebook = notebook || this.notebookTracker?.currentWidget.content;
    notebook.widgets.forEach((cell: Cell) => {
      if (cell.model.type === 'code') {
        if (!selectedOnly || notebook?.isSelectedOrActive(cell)) {
          codeCells.push(cell as CodeCell);
        }
      }
    });
    return codeCells;
  }

  private tryReplacing(
    cell: CodeCell,
    cellText: string,
    extractor: RegExpForeignCodeExtractor
  ) {
    const extracted = extractor.extractForeignCode(cellText);
    const cellEditor = cell.editor;
    if (
      cellEditor &&
      extracted &&
      extracted.length > 0 &&
      extracted[0].foreignCode &&
      extracted[0].range
    ) {
      const sqlText = extracted[0].foreignCode;
      const formattedSql = this.sqlFormatter.format(sqlText) + '\n';
      cell.model.sharedModel.updateSource(
        cellEditor?.getOffsetAt(extracted[0].range.start) || 0,
        cellEditor?.getOffsetAt(extracted[0].range.end) || 0,
        formattedSql
      );
    }
    return;
  }

  private async formatCells(selectedOnly: boolean, notebook?: Notebook) {
    if (this.working || !this.applicable()) {
      return;
    }
    try {
      this.working = true;
      const selectedCells = this.getCodeCells(selectedOnly, notebook);
      if (selectedCells.length > 0) {
        const currentTexts = selectedCells.map(cell =>
          cell.model.sharedModel.getSource()
        );
        for (let i = 0; i < selectedCells.length; ++i) {
          const cell = selectedCells[i];
          const currentText = currentTexts[i];
          if (cell.model.sharedModel.getSource() === currentText) {
            for (let i = 0; i < this.extractors.length; ++i) {
              this.tryReplacing(cell, currentText, this.extractors[i]);
            }
          }
        }
      }
    } catch (error: any) {
      await showErrorMessage('Jupyterlab Code Formatter Error', error);
    } finally {
      this.working = false;
    }
  }

  applicable() {
    const selectedCells = this.getCodeCells();
    if (selectedCells.length > 0) {
      const currentTexts = selectedCells.map(cell =>
        cell.model.sharedModel.getSource()
      );
      let numSqlCells = 0;
      currentTexts.forEach(cellText => {
        const found = this.extractors.find(extractor =>
          extractor.hasForeignCode(cellText)
        );
        if (found) {
          numSqlCells++;
        }
      });
      // eslint-disable-next-line eqeqeq
      return numSqlCells == selectedCells.length;
    }
    return false;
  }
}

class JupyterlabFileEditorCodeFormatter {
  private editorTracker: IEditorTracker;
  private working: boolean;
  private sqlFormatter: SqlFormatter;
  constructor(editorTracker: IEditorTracker, sqlFormatter: SqlFormatter) {
    this.working = false;
    this.editorTracker = editorTracker;
    this.sqlFormatter = sqlFormatter;
  }

  setFormatter(sqlFormatter: SqlFormatter) {
    this.sqlFormatter = sqlFormatter;
  }

  formatAction() {
    if (this.working) {
      return;
    }
    const editorWidget = this.editorTracker.currentWidget;
    if (editorWidget) {
      try {
        this.working = true;
        const editor = editorWidget.content.editor;
        const code = editor?.model.sharedModel.getSource();
        const formatted = this.sqlFormatter.format(code);
        editorWidget.content.editor.model.sharedModel.setSource(formatted);
      } finally {
        this.working = false;
      }
    }
  }
}

export class JupyterLabCodeFormatter {
  private app: JupyterFrontEnd;
  private tracker: INotebookTracker;
  private editorTracker: IEditorTracker;
  private notebookCodeFormatter: JupyterlabNotebookCodeFormatter;
  private fileEditorCodeFormatter: JupyterlabFileEditorCodeFormatter;
  constructor(
    app: JupyterFrontEnd,
    tracker: INotebookTracker,
    editorTracker: IEditorTracker,
    sqlFormatter: SqlFormatter
  ) {
    this.app = app;
    this.tracker = tracker;
    this.editorTracker = editorTracker;
    this.notebookCodeFormatter = new JupyterlabNotebookCodeFormatter(
      this.tracker,
      sqlFormatter
    );
    this.fileEditorCodeFormatter = new JupyterlabFileEditorCodeFormatter(
      this.editorTracker,
      sqlFormatter
    );
    this.setupCommands();
    this.setupContextMenu();
  }

  setFormatter(sqlFormatter: SqlFormatter): void {
    this.notebookCodeFormatter.setFormatter(sqlFormatter);
    this.fileEditorCodeFormatter.setFormatter(sqlFormatter);
  }

  pushExtractors(
    sparksqlStartMarker: string,
    sparksqlEndMarker: string,
    trinoStartMarker: string,
    trinoEndMarker: string
  ) {
    this.notebookCodeFormatter.pushExtractors(
      sparksqlStartMarker,
      sparksqlEndMarker,
      trinoStartMarker,
      trinoEndMarker
    );
  }

  private setupContextMenu() {
    this.app.contextMenu.addItem({
      command: Constants.FORMAT_COMMAND,
      selector: '.jp-CodeCell'
    });
    this.app.contextMenu.addItem({
      command: Constants.FORMAT_COMMAND_DOCUMENT,
      selector: '.jp-FileEditor'
    });
  }

  private setupCommands() {
    this.app.commands.addCommand(Constants.FORMAT_COMMAND, {
      execute: async () => {
        await this.notebookCodeFormatter.formatSelectedCodeCells();
      },
      isVisible: () => {
        return this.notebookCodeFormatter.applicable();
      },
      label: 'Format SQL Cell'
    });
    this.app.commands.addCommand(Constants.FORMAT_COMMAND_DOCUMENT, {
      execute: async () => {
        await this.fileEditorCodeFormatter.formatAction();
      },
      label: 'Format SQL Document'
    });
  }
}
