import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { ISettingRegistry } from '@jupyterlab/settingregistry';
import { ILSPCodeExtractorsManager } from '@jupyter-lsp/jupyterlab-lsp';
import { ICodeMirror } from '@jupyterlab/codemirror';
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
  sqlCodeMirrorModesFor
} from './utils';
import { Constants } from './constants';
import { KeywordCase } from 'sql-formatter';

/*
Results in

LINE_MAGIC_EXTRACT
(?:^|\n)%sparksql(?: |-c|--cache|-e|--eager|-[a-z] [0-9a-zA-Z/._]+|--[a-zA-Z]+ [0-9a-zA-Z/._]+)*([^\n]*)

CELL_MAGIC_EXTRACT
(?:^|\n)%%sparksql(?: |-c|--cache|-e|--eager|-[a-z] [0-9a-zA-Z/._]+|--[a-zA-Z]+ [0-9a-zA-Z/._]+)*\n([^]*)
*/

/**
 * Code taken from https://github.com/jupyterlab/jupyterlab/blob/master/packages/codemirror/src/codemirror-ipython.ts
 * Modified to support embedded sql syntax
 */
function codeMirrorWithSqlSyntaxHighlightSupport(c: ICodeMirror) {
  /**
   * Define an IPython codemirror mode.
   *
   * It is a slightly altered Python Mode with a `?` operator.
   */
  c.CodeMirror.defineMode(
    'ipython',
    (config: CodeMirror.EditorConfiguration, modeOptions?: any) => {
      const pythonConf: any = {};
      for (const prop in modeOptions) {
        if (modeOptions.hasOwnProperty(prop)) {
          pythonConf[prop] = modeOptions[prop];
        }
      }
      pythonConf.name = 'python';
      pythonConf.singleOperators = new RegExp('^[\\+\\-\\*/%&|@\\^~<>!\\?]');
      pythonConf.identifiers = new RegExp(
        '^[_A-Za-z\u00A1-\uFFFF][_A-Za-z0-9\u00A1-\uFFFF]*'
      );
      //return c.CodeMirror.getMode(config, pythonConf);

      // Instead of returning this mode we multiplex it with SQL
      const pythonMode = c.CodeMirror.getMode(config, pythonConf);

      // get a mode for SQL
      const sqlMode = c.CodeMirror.getMode(config, 'sql');

      // multiplex python with SQL and return it
      const multiplexedModes = sqlCodeMirrorModesFor(
        'sparksql',
        sqlMode
      ).concat(sqlCodeMirrorModesFor('trino', sqlMode));

      return c.CodeMirror.multiplexingMode(pythonMode, ...multiplexedModes);
    }
    // Original code has a third argument. Not sure why we don't..
    // https://github.com/jupyterlab/jupyterlab/blob/master/packages/codemirror/src/codemirror-ipython.ts
    // ,
    // 'python'
  );

  // The following is already done by default implementation so not redoing here
  // c.CodeMirror.defineMIME('text/x-ipython', 'ipython');
  // c.CodeMirror.modeInfo.push({
  //   ext: [],
  //   mime: 'text/x-ipython',
  //   mode: 'ipython',
  //   name: 'ipython'
  // });
}

/**
 * Initialization data for the jupyterlab-sql-editor extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'jupyterlab-sql-editor:plugin',
  autoStart: true,
  optional: [],
  requires: [
    ICodeMirror,
    ILSPCodeExtractorsManager,
    ISettingRegistry,
    IEditorTracker,
    INotebookTracker
  ],
  activate: (
    app: JupyterFrontEnd,
    codeMirror: ICodeMirror,
    lspExtractorsMgr: ILSPCodeExtractorsManager,
    settingRegistry: ISettingRegistry,
    editorTracker: IEditorTracker,
    tracker: INotebookTracker
  ) => {
    console.log('JupyterLab extension jupyterlab-sql-editor is activated!');

    const sqlFormatter = new SqlFormatter(4, false, 'upper');
    const sqlCodeFormatter = new SqlCodeFormatter(
      app,
      tracker,
      editorTracker,
      codeMirror,
      sqlFormatter
    );
    console.log('jupyterlab-sql-editor SQL code formatter registered');

    /**
     * Load the settings for this extension
     *
     * @param setting Extension settings
     */
    function loadSetting(settings: ISettingRegistry.ISettings): void {
      // Read the settings and convert to the correct type
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
    }

    // Wait for the application to be restored and
    // for the settings for this plugin to be loaded
    Promise.all([
      app.restored,
      settingRegistry.load(Constants.SETTINGS_SECTION)
    ])
      .then(([, settings]) => {
        // Read the settings
        loadSetting(settings);
        // Listen for your plugin setting changes using Signal
        settings.changed.connect(loadSetting);
      })
      .catch(reason => {
        console.error(
          `Something went wrong when reading the settings.\n${reason}`
        );
      });

    // JupyterLab uses the CodeMirror library to syntax highlight code
    // within the cells. Register a multiplex CodeMirror capable of
    // highlightin SQL which is embedded in a IPython magic or within
    // a python string (delimited by markers)
    codeMirrorWithSqlSyntaxHighlightSupport(codeMirror);
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
  }
};

export default plugin;
