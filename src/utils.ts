import { RegExpForeignCodeExtractor } from '@jupyter-lsp/jupyterlab-lsp';

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

export function lineMagicExtractor(
  language: string
): RegExpForeignCodeExtractor {
  return new RegExpForeignCodeExtractor({
    language: language,
    pattern: `${BEGIN}%${language}${COMMANDS}([^\n]*)`,
    foreignCaptureGroups: [1],
    isStandalone: false,
    fileExtension: language
  });
}

export function cellMagicExtractor(
  language: string
): RegExpForeignCodeExtractor {
  return new RegExpForeignCodeExtractor({
    language: language,
    pattern: `^%%(${language})( .*?)?\n([^]*)`,
    foreignCaptureGroups: [3],
    isStandalone: false,
    fileExtension: language
  });
}

export function markerExtractor(
  startMarker: string,
  endMarker: string,
  language: string
): RegExpForeignCodeExtractor {
  return new RegExpForeignCodeExtractor({
    language: language,
    pattern: `${startMarker}.*?\n([\\S\\s]*)${endMarker}`,
    foreignCaptureGroups: [1],
    isStandalone: false,
    fileExtension: language
  });
}
