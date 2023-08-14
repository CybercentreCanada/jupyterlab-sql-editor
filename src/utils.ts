import { RegExpForeignCodeExtractor } from '@jupyter-lsp/jupyterlab-lsp';
import { Mode } from 'codemirror';
import { ICodeMirror } from '@jupyterlab/codemirror';

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
    foreign_capture_groups: [1],
    is_standalone: true,
    file_extension: language
  });
}

export function cellMagicExtractor(
  language: string
): RegExpForeignCodeExtractor {
  return new RegExpForeignCodeExtractor({
    language: language,
    pattern: `^${cell_magic(language)}.*?\n([\\S\\s]*)`,
    foreign_capture_groups: [1],
    is_standalone: true,
    file_extension: language
  });
}

export function markerExtractor(language: string): RegExpForeignCodeExtractor {
  return new RegExpForeignCodeExtractor({
    language: language,
    pattern: `${start(language)}.*?\n([\\S\\s]*)${end(language)}`,
    foreign_capture_groups: [1],
    is_standalone: true,
    file_extension: language
  });
}

/**
 * Register text editor based on file type.
 * @param c
 * @param language
 */
export function registerCodeMirrorFor(c: ICodeMirror, language: string): void {
  c.CodeMirror.defineMode(
    language,
    (config: CodeMirror.EditorConfiguration, modeOptions?: any) => {
      const mode = c.CodeMirror.getMode(config, 'sql');
      return mode;
    }
  );
  c.CodeMirror.defineMIME(`text/x-${language}`, language);
  c.CodeMirror.modeInfo.push({
    ext: [language],
    mime: `text/x-${language}`,
    mode: language,
    name: language
  });
}
