import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { ISettingRegistry } from '@jupyterlab/settingregistry';
import { ILSPCodeExtractorsManager } from '@jupyterlab/lsp';
import { IEditorLanguageRegistry } from '@jupyterlab/codemirror';
import { INotebookTracker } from '@jupyterlab/notebook';
import { IEditorTracker } from '@jupyterlab/fileeditor';
import {
  JupyterLabCodeFormatter as SqlCodeFormatter,
  SqlFormatter
} from './formatter';
import {
  cellMagicExtractor,
  markerExtractor,
  lineMagicExtractor,
  registerCodeMirrorFor
} from './utils';
import { KeywordCase } from 'sql-formatter';

const JUPYTERLAB_SQL_EDITOR_PLUGIN = 'jupyterlab-sql-editor:plugin';

/**
 * Initialization data for the jupyterlab-sql-editor extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: JUPYTERLAB_SQL_EDITOR_PLUGIN,
  description:
    'SQL editor support for formatting, syntax highlighting and code completion of SQL in cell magic, line magic, python string and file editor.',
  autoStart: true,
  optional: [],
  requires: [
    ISettingRegistry,
    ILSPCodeExtractorsManager,
    INotebookTracker,
    IEditorTracker,
    IEditorLanguageRegistry
  ],
  activate: (
    app: JupyterFrontEnd,
    settings: ISettingRegistry,
    lspExtractorsMgr: ILSPCodeExtractorsManager,
    notebookTracker: INotebookTracker,
    editorTracker: IEditorTracker,
    languages: IEditorLanguageRegistry
  ): void => {
    const updateSettings = (
      settings: ISettingRegistry.ISettings,
      sqlCodeFormatter: SqlCodeFormatter
    ) => {
      const formatTabwidth = settings.get('formatTabWidth').composite as number;
      const formatUseTabs = settings.get('formatUseTabs').composite as boolean;
      const formatKeywordCase = settings.get('formatKeywordCase')
        .composite as KeywordCase;
      const sqlFormatter = new SqlFormatter(
        formatTabwidth,
        formatUseTabs,
        formatKeywordCase
      );
      sqlCodeFormatter.setFormatter(sqlFormatter);
      console.log('jupyterlab-sql-editor SQL code formatter registered');
    };

    app.restored
      .then(() => {
        // JupyterLab uses the CodeMirror library to syntax highlight code
        // within the cells. Register a multiplex CodeMirror capable of
        // highlightin SQL which is embedded in a IPython magic or within
        // a python string (delimited by markers)
        registerCodeMirrorFor(languages, 'Spark language server', 'sparksql');
        registerCodeMirrorFor(languages, 'Trino language server', 'trino');
        console.log(
          'jupyterlab-sql-editor code mirror for syntax highlighting registered'
        );

        // JupyterLab-LSP relies on extractors to pull the SQL out of the cell
        // and into a virtual document which is then passed to the sql-language-server
        // for code completion evaluation
        lspExtractorsMgr.register(markerExtractor('sparksql'), 'python');
        lspExtractorsMgr.register(lineMagicExtractor('sparksql'), 'python');
        lspExtractorsMgr.register(cellMagicExtractor('sparksql'), 'python');
        lspExtractorsMgr.register(markerExtractor('trino'), 'python');
        lspExtractorsMgr.register(lineMagicExtractor('trino'), 'python');
        lspExtractorsMgr.register(cellMagicExtractor('trino'), 'python');
        console.log('jupyterlab-sql-editor LSP extractors registered');

        const settingsPromise = settings.load(JUPYTERLAB_SQL_EDITOR_PLUGIN);
        const sqlCodeFormatter = new SqlCodeFormatter(
          app,
          notebookTracker,
          editorTracker,
          new SqlFormatter(4, false, 'upper')
        );
        console.log('jupyterlab-sql-editor sqlCodeFormatter initialized');

        settingsPromise
          .then(settingValues => {
            updateSettings(settingValues, sqlCodeFormatter);
            settingValues.changed.connect(newSettings => {
              updateSettings(newSettings, sqlCodeFormatter);
            });
          })
          .catch(console.error);
      })
      .catch(console.error);
    console.log('JupyterLab extension jupyterlab-sql-editor is activated');
  }
};

export default plugin;
