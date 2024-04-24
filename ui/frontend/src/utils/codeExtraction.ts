import { CodeArtifact, DAGTemplateWithData } from "../state/api/friendlyApi";

/**
 * Simple function to extract contents from code.
 * We have a list of files that we slurp up (these
 * are given to us currently on the server side, but in
 * the future we may need to fetch them directly from the blob store.
 * @param codeArtifact
 * @param projectVersion
 * @returns
 */
export const extractCodeContents = (
  codeArtifact: CodeArtifact,
  dagTemplate: DAGTemplateWithData | undefined
): string | undefined => {
  const { start, end, path } = codeArtifact;
  if (dagTemplate === undefined) {
    return undefined;
  }
  const availableFiles =
    dagTemplate.code?.files.filter((file) => file.path === path) || [];
  if (availableFiles.length === 0) {
    return undefined;
  }
  const fileContents = availableFiles[0].contents;
  const lines = fileContents.split("\n");
  if (end > lines.length) {
    return undefined;
  }
  return lines.slice(start, end).join("\n");
};

/**
 * This is inefficient, but its easy.
 * TODO -- make this do one pass, should be simple.
 * @param codeArtifacts
 * @param projectVersion
 * @returns
 */
export const extractAllCodeContents = (
  codeArtifacts: CodeArtifact[],
  projectVersion: DAGTemplateWithData | undefined
): (string | undefined)[] => {
  return codeArtifacts.map((codeArtifact) => {
    return extractCodeContents(codeArtifact, projectVersion);
  });
};
