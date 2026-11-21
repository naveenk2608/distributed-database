// The dashboard's theme: dark unless the viewer picked light, which is kept
// in localStorage (index.html applies it before the first paint).
import { useCallback, useEffect, useState } from "react";

export type Theme = "dark" | "light";

export const themeKey = "dkv-theme";

function stored(): Theme {
  try {
    return localStorage.getItem(themeKey) === "light" ? "light" : "dark";
  } catch {
    return "dark"; // storage blocked: the default
  }
}

export function useTheme(): [Theme, () => void] {
  const [theme, setTheme] = useState<Theme>(stored);
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);
  const toggle = useCallback(() => {
    setTheme((t) => {
      const next: Theme = t === "dark" ? "light" : "dark";
      try {
        localStorage.setItem(themeKey, next);
      } catch {
        // Not saved; the choice still holds until the page reloads.
      }
      return next;
    });
  }, []);
  return [theme, toggle];
}

// useNow re-renders its caller every interval with the current time, for
// relative times ("12s ago").
export function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}
