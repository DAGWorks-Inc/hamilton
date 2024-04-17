import { useState, useEffect } from "react";

const getStorageValue = (
  key: string,
  defaultValue: string | undefined
): string | undefined => {
  const saved = localStorage.getItem(key);
  const initial = saved !== undefined ? saved : undefined;
  return initial || defaultValue;
};

export const useLocalStorage = (
  key: string,
  defaultValue: string | undefined
): [
  string | undefined,
  React.Dispatch<React.SetStateAction<string | undefined>>
] => {
  const [value, setValue] = useState(() => {
    return getStorageValue(key, defaultValue);
  });

  useEffect(() => {
    if (value === undefined) {
      localStorage.removeItem(key);
    } else {
      localStorage.setItem(key, value);
    }
  }, [key, value]);

  return [value, setValue];
};
