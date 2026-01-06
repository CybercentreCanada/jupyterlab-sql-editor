import { RegExpForeignCodeExtractor } from '@jupyter-lsp/jupyterlab-lsp';
import { JupyterFrontEnd } from '@jupyterlab/application';
import { showErrorMessage } from '@jupyterlab/apputils';
import { Cell, CodeCell } from '@jupyterlab/cells';
import { IEditorTracker } from '@jupyterlab/fileeditor';
import { INotebookTracker, Notebook } from '@jupyterlab/notebook';
import { format, KeywordCase, SqlLanguage } from 'sql-formatter';
import { Constants } from './constants';
import { cellMagicExtractor, markerExtractor } from './utils';

type DialectedExtractor = {
  dialect: 'sparksql' | 'trino';
  extractor: RegExpForeignCodeExtractor;
};

export class SqlFormatter {
  private dialect: SqlLanguage;
  private formatTabWidth: number;
  private formatUseTabs: boolean;
  private formatKeywordCase: KeywordCase;

  constructor(
    dialect: SqlLanguage,
    formatTabWidth: number,
    formatUseTabs: boolean,
    formatKeywordCase: KeywordCase
  ) {
    this.dialect = dialect;
    this.formatTabWidth = formatTabWidth;
    this.formatUseTabs = formatUseTabs;
    this.formatKeywordCase = formatKeywordCase;
  }

  format(text: string | null): string {
    const formatted = format(text || '', {
      language: this.dialect, // Defaults to "sql" (see the above list of supported dialects)
      tabWidth: this.formatTabWidth, // Defaults to two spaces. Ignored if useTabs is true
      useTabs: this.formatUseTabs, // Defaults to false
      keywordCase: this.formatKeywordCase, // Defaults to false (not safe to use when SQL dialect has case-sensitive identifiers)
      linesBetweenQueries: 2 // Defaults to 1
    });
    return formatted;
  }
}

type FormatterRegistry = Map<string, SqlFormatter>;

class JupyterlabNotebookCodeFormatter {
  private notebookTracker: INotebookTracker;
  private working: boolean;
  private extractors: DialectedExtractor[];
  private formatters: FormatterRegistry;

  constructor(
    notebookTracker: INotebookTracker,
    formatters: FormatterRegistry
  ) {
    this.working = false;
    this.notebookTracker = notebookTracker;
    this.extractors = [];
    this.extractors.push({
      dialect: 'sparksql',
      extractor: cellMagicExtractor('sparksql')
    });
    this.extractors.push({
      dialect: 'trino',
      extractor: cellMagicExtractor('trino')
    });
    this.formatters = formatters;
  }

  setFormatters(formatters: FormatterRegistry) {
    this.formatters = formatters;
  }

  pushExtractors(
    sparksqlStartMarker: string,
    sparksqlEndMarker: string,
    trinoStartMarker: string,
    trinoEndMarker: string
  ) {
    this.extractors.push({
      dialect: 'sparksql',
      extractor: markerExtractor(
        sparksqlStartMarker,
        sparksqlEndMarker,
        'sparksql'
      )
    });
    this.extractors.push({
      dialect: 'trino',
      extractor: markerExtractor(trinoStartMarker, trinoEndMarker, 'trino')
    });
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
    wrapped: DialectedExtractor
  ) {
    const extracted = wrapped.extractor.extractForeignCode(cellText);
    const cellEditor = cell.editor;
    if (
      cellEditor &&
      extracted &&
      extracted.length > 0 &&
      extracted[0].foreignCode &&
      extracted[0].range
    ) {
      const formatter = this.formatters.get(wrapped.dialect);
      if (!formatter) {
        return;
      }

      const formattedSql = formatter.format(extracted[0].foreignCode) + '\n';

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
        const found = this.extractors.find(wrapped =>
          wrapped.extractor.hasForeignCode(cellText)
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
    formatters: FormatterRegistry,
    defaultSqlFormatter: SqlFormatter
  ) {
    this.app = app;
    this.tracker = tracker;
    this.editorTracker = editorTracker;
    this.notebookCodeFormatter = new JupyterlabNotebookCodeFormatter(
      this.tracker,
      formatters
    );
    this.fileEditorCodeFormatter = new JupyterlabFileEditorCodeFormatter(
      this.editorTracker,
      defaultSqlFormatter
    );
    this.setupCommands();
    this.setupContextMenu();
  }

  setFormatters(
    formatters: FormatterRegistry,
    defaultSqlFormatter: SqlFormatter
  ): void {
    this.notebookCodeFormatter.setFormatters(formatters);
    this.fileEditorCodeFormatter.setFormatter(defaultSqlFormatter);
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
