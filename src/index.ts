import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { IEditorLanguageRegistry } from '@jupyterlab/codemirror';
import { IEditorTracker } from '@jupyterlab/fileeditor';
import { ILSPCodeExtractorsManager } from '@jupyterlab/lsp';
import { INotebookTracker } from '@jupyterlab/notebook';
import { ISettingRegistry } from '@jupyterlab/settingregistry';
import { KeywordCase } from 'sql-formatter';
import {
  JupyterLabCodeFormatter as SqlCodeFormatter,
  SqlFormatter
} from './formatter';
import {
  cellMagicExtractor,
  lineMagicExtractor,
  markerExtractor
} from './utils';

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
      const sparksqlStartMarker = settings.get('sparksqlStartMarker')
        .composite as string;
      const sparksqlEndMarker = settings.get('sparksqlEndMarker')
        .composite as string;
      const trinoStartMarker = settings.get('trinoStartMarker')
        .composite as string;
      const trinoEndMarker = settings.get('trinoEndMarker').composite as string;
      const formatters = new Map<string, SqlFormatter>();
      formatters.set(
        'sparksql',
        new SqlFormatter(
          'spark',
          formatTabwidth,
          formatUseTabs,
          formatKeywordCase
        )
      );
      formatters.set(
        'trino',
        new SqlFormatter(
          'trino',
          formatTabwidth,
          formatUseTabs,
          formatKeywordCase
        )
      );
      const defaultSqlFormatter = new SqlFormatter(
        'sql',
        formatTabwidth,
        formatUseTabs,
        formatKeywordCase
      );
      lspExtractorsMgr.register(
        markerExtractor(sparksqlStartMarker, sparksqlEndMarker, 'sparksql'),
        'python'
      );
      lspExtractorsMgr.register(
        markerExtractor(trinoStartMarker, trinoEndMarker, 'trino'),
        'python'
      );
      sqlCodeFormatter.setFormatters(formatters, defaultSqlFormatter);
      sqlCodeFormatter.pushExtractors(
        sparksqlStartMarker,
        sparksqlEndMarker,
        trinoStartMarker,
        trinoEndMarker
      );
      console.log('jupyterlab-sql-editor SQL code formatter registered');
    };

    app.restored
      .then(() => {
        // Add sparksql and trino languages for syntax highlighting
        languages.addLanguage({
          name: 'sparksql',
          displayName: 'Spark language server',
          mime: ['application/sparksql', 'text/x-sparksql'],
          extensions: ['sparksql'],
          async load() {
            const m = await import('@codemirror/lang-sql');
            return m.sql();
          }
        });
        languages.addLanguage({
          name: 'trino',
          displayName: 'Trino language server',
          mime: ['application/trino', 'text/x-trino'],
          extensions: ['trino'],
          async load() {
            const m = await import('@codemirror/lang-sql');
            return m.sql();
          }
        });
        console.log(
          'jupyterlab-sql-editor: languages added for syntax highlighting'
        );

        // JupyterLab-LSP relies on extractors to pull the SQL out of the cell
        // and into a virtual document which is then passed to the sql-language-server
        // for code completion evaluation
        lspExtractorsMgr.register(lineMagicExtractor('sparksql'), 'python');
        lspExtractorsMgr.register(cellMagicExtractor('sparksql'), 'python');
        lspExtractorsMgr.register(lineMagicExtractor('trino'), 'python');
        lspExtractorsMgr.register(cellMagicExtractor('trino'), 'python');
        console.log('jupyterlab-sql-editor: LSP extractors registered');

        const formatTabWidth = 4;
        const formatUseTabs = false;
        const formatKeywordCase: KeywordCase = 'upper';
        const formatters = new Map<string, SqlFormatter>();
        formatters.set(
          'sparksql',
          new SqlFormatter(
            'spark',
            formatTabWidth,
            formatUseTabs,
            formatKeywordCase
          )
        );
        formatters.set(
          'trino',
          new SqlFormatter(
            'trino',
            formatTabWidth,
            formatUseTabs,
            formatKeywordCase
          )
        );
        const defaultSqlFormatter = new SqlFormatter(
          'sql',
          formatTabWidth,
          formatUseTabs,
          formatKeywordCase
        );
        const sqlCodeFormatter = new SqlCodeFormatter(
          app,
          notebookTracker,
          editorTracker,
          formatters,
          defaultSqlFormatter
        );
        console.log('jupyterlab-sql-editor: sqlCodeFormatter initialized');

        const settingsPromise = settings.load(JUPYTERLAB_SQL_EDITOR_PLUGIN);
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
    console.log('jupyterlab-sql-editor is activated');
  }
};

export default plugin;
