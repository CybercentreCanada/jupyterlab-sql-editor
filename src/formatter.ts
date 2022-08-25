import { Cell, CodeCell } from '@jupyterlab/cells';
import { INotebookTracker, Notebook } from '@jupyterlab/notebook';
import { IEditorTracker } from '@jupyterlab/fileeditor';
import { showErrorMessage } from '@jupyterlab/apputils';
import { format } from 'sql-formatter';
import { RegExpForeignCodeExtractor } from '@krassowski/jupyterlab-lsp';
import { Constants } from './constants';
import { JupyterFrontEnd } from '@jupyterlab/application';
import { cellMagicExtractor, markerExtractor } from './utils';
import { ICodeMirror } from '@jupyterlab/codemirror';
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
  private codeMirror: ICodeMirror;
  private sqlFormatter: SqlFormatter;
  constructor(
    notebookTracker: INotebookTracker,
    codeMirror: ICodeMirror,
    sqlFormatter: SqlFormatter
  ) {
    this.working = false;
    this.notebookTracker = notebookTracker;
    this.extractors = [];
    this.extractors.push(cellMagicExtractor('sparksql'));
    this.extractors.push(cellMagicExtractor('trino'));
    this.extractors.push(markerExtractor('sparksql'));
    this.extractors.push(markerExtractor('trino'));
    this.codeMirror = codeMirror;
    this.sqlFormatter = sqlFormatter;
  }

  setFormatter(sqlFormatter: SqlFormatter) {
    this.sqlFormatter = sqlFormatter;
  }

  public async formatAction() {
    return this.formatCells(true);
  }

  public async formatSelectedCodeCells(notebook?: Notebook) {
    return this.formatCells(true, notebook);
  }

  private getCodeCells(selectedOnly = true, notebook?: Notebook): CodeCell[] {
    if (!this.notebookTracker.currentWidget) {
      return [];
    }
    const codeCells: CodeCell[] = [];
    notebook = notebook || this.notebookTracker.currentWidget.content;
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
    cellText: string,
    extractor: RegExpForeignCodeExtractor
  ): string | null {
    const extracted = extractor.extract_foreign_code(cellText);
    if (
      extracted &&
      extracted.length > 0 &&
      extracted[0].foreign_code &&
      extracted[0].range
    ) {
      const sqlText = extracted[0].foreign_code;
      const formattedSql = this.sqlFormatter.format(sqlText) + '\n';
      const doc = new this.codeMirror.CodeMirror.Doc(cellText, 'sql', 0, '\n');
      const startPos = new this.codeMirror.CodeMirror.Pos(
        extracted[0].range.start.line,
        extracted[0].range.start.column
      );
      const endPos = new this.codeMirror.CodeMirror.Pos(
        extracted[0].range.end.line,
        extracted[0].range.end.column
      );
      doc.replaceRange(formattedSql, startPos, endPos);
      return doc.getValue();
    }
    return null;
  }

  private async formatCells(selectedOnly: boolean, notebook?: Notebook) {
    if (this.working || !this.applicable()) {
      return;
    }
    try {
      this.working = true;
      const selectedCells = this.getCodeCells(selectedOnly, notebook);
      if (selectedCells.length > 0) {
        const currentTexts = selectedCells.map(cell => cell.model.value.text);
        const formattedTexts = currentTexts.map(cellText => {
          const formatted = this.extractors
            .map(extractor => this.tryReplacing(cellText, extractor))
            .find(formatted => formatted);
          return formatted || '';
        });
        for (let i = 0; i < selectedCells.length; ++i) {
          const cell = selectedCells[i];
          const currentText = currentTexts[i];
          const formattedText = formattedTexts[i];
          if (cell.model.value.text === currentText) {
            cell.model.value.text = formattedText;
          }
        }
      }
    } catch (error) {
      await showErrorMessage('Jupyterlab Code Formatter Error', error);
    } finally {
      this.working = false;
    }
  }

  applicable() {
    const selectedCells = this.getCodeCells();
    if (selectedCells.length > 0) {
      const currentTexts = selectedCells.map(cell => cell.model.value.text);
      let numSqlCells = 0;
      currentTexts.forEach(cellText => {
        const found = this.extractors.find(extractor =>
          extractor.has_foreign_code(cellText)
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
        const code = editor?.model.value.text;
        const formatted = this.sqlFormatter.format(code);
        editorWidget.content.editor.model.value.text = formatted;
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
    codeMirror: ICodeMirror,
    sqlFormatter: SqlFormatter
  ) {
    this.app = app;
    this.tracker = tracker;
    this.editorTracker = editorTracker;
    this.notebookCodeFormatter = new JupyterlabNotebookCodeFormatter(
      this.tracker,
      codeMirror,
      sqlFormatter
    );
    this.fileEditorCodeFormatter = new JupyterlabFileEditorCodeFormatter(
      this.editorTracker,
      sqlFormatter
    );
    this.setupCommands();
    this.setupContextMenu();
  }

  setFormatter(sqlFormatter: SqlFormatter) {
    this.notebookCodeFormatter.setFormatter(sqlFormatter);
    this.fileEditorCodeFormatter.setFormatter(sqlFormatter);
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
      label: 'Format Sql Cell'
    });
    this.app.commands.addCommand(Constants.FORMAT_COMMAND_DOCUMENT, {
      execute: async () => {
        await this.fileEditorCodeFormatter.formatAction();
      },
      label: 'Format Sql Document'
    });
  }
}
