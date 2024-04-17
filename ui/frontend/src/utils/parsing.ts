/**
 * Parses a string of comma-separated integers into an array of integers.
 * This is used in building URL parameters for comparing versions, etc...
 * @param str
 * @returns
 */
export const parseListOfInts = (str: string) => {
  if (!str) {
    return [];
  }
  return str.split(",").map((s) => parseInt(s));
};
