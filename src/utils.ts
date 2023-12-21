import { RegExpForeignCodeExtractor } from '@jupyter-lsp/jupyterlab-lsp';
import { Mode } from 'codemirror';
import { IEditorLanguageRegistry } from '@jupyterlab/codemirror';

function line_magic(language: string) {
  return `%${language}`;
}
function cell_magic(language: string) {
  return `%%${language}`;
}
function start(language: string) {
  return `--start-${language}`;
}
function end(language: string) {
  return `--end-${language}`;
}

// sparksql magic accepts options in the long form
// --dataframe df
// or in the short form
// -d df
// some options do not require any values, they act more as a flag
const SPACE = ' ';
const OPTION_VALUE = '[0-9a-zA-Z\\._]+';
const SHORT_OPTS = '-[a-z]';
const LONG_OPTS = '--[_a-zA-Z]+';
const COMMANDS = `(?:${SPACE}|${SHORT_OPTS} ${OPTION_VALUE}|${LONG_OPTS} ${OPTION_VALUE}|${SHORT_OPTS}|${LONG_OPTS})*`;
const BEGIN = '(?:^|\n)';

export function sqlCodeMirrorModesFor(
  language: string,
  sqlMode: Mode<unknown>
) {
  return [
    {
      open: `${start(language)}`,
      close: `${end(language)}`,
      // parseDelimiters is set to true which considers
      // the marker as part of the SQL statement
      // it is thus syntax highlighted as a comment
      parseDelimiters: true,
      mode: sqlMode
    },
    {
      open: RegExp(`${line_magic(language)}${COMMANDS}`) as unknown as string,
      close: '\n', // Line magic: Stop at end of line (blank line)
      parseDelimiters: false,
      mode: sqlMode
    },
    {
      open: RegExp(`${cell_magic(language)}${COMMANDS}`) as unknown as string,
      close: '__A MARKER THAT WILL NEVER BE MATCHED__', // Cell magic: capture chars till the end of the cell
      parseDelimiters: false,
      mode: sqlMode
    }
  ];
}

export function lineMagicExtractor(
  language: string
): RegExpForeignCodeExtractor {
  return new RegExpForeignCodeExtractor({
    language: language,
    pattern: `${BEGIN}${line_magic(language)}${COMMANDS}([^\n]*)`,
    foreignCaptureGroups: [1],
    isStandalone: true,
    fileExtension: language
  });
}

export function cellMagicExtractor(
  language: string
): RegExpForeignCodeExtractor {
  return new RegExpForeignCodeExtractor({
    language: language,
    pattern: `^${cell_magic(language)}.*?\n([\\S\\s]*)`,
    foreignCaptureGroups: [1],
    isStandalone: true,
    fileExtension: language
  });
}

export function markerExtractor(language: string): RegExpForeignCodeExtractor {
  return new RegExpForeignCodeExtractor({
    language: language,
    pattern: `${start(language)}.*?\n([\\S\\s]*)${end(language)}`,
    foreignCaptureGroups: [1],
    isStandalone: true,
    fileExtension: language
  });
}

export function registerCodeMirrorFor(
  languageRegistry: IEditorLanguageRegistry,
  displayName: string,
  language: string
): void {
  languageRegistry.addLanguage({
    name: language,
    displayName: displayName,
    mime: [`application/${language}`, `text/x-${language}`],
    extensions: [language],
    async load() {
      const m = await import('@codemirror/lang-sql');
      return m.sql();
    }
  });
}
