// The frame around the pages: the live stream's state, the banner when it
// drops, and the theme toggle. EventSource is the transport seam, so a fake
// one stands in for the console's stream.
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { overviewKey, type Overview } from "./api";
import { Layout } from "./components/Layout";
import { clock } from "./format";
import { themeKey } from "./theme";
import overviewJSON from "./fixtures/overview.json";

const overview = overviewJSON as Overview;

class FakeEventSource {
  static last: FakeEventSource | undefined;
  onerror: (() => void) | null = null;
  private listeners = new Map<string, (m: MessageEvent<string>) => void>();
  constructor(readonly url: string) {
    FakeEventSource.last = this;
  }
  addEventListener(type: string, fn: (m: MessageEvent<string>) => void) {
    this.listeners.set(type, fn);
  }
  close() {}
  send(ov: Overview) {
    this.listeners.get("overview")?.(new MessageEvent("overview", { data: JSON.stringify(ov) }));
  }
}

beforeEach(() => {
  globalThis.EventSource = FakeEventSource as unknown as typeof EventSource;
  localStorage.clear();
  delete document.documentElement.dataset.theme;
});

function show() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  qc.setQueryData(overviewKey, overview);
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route index element={<p>page</p>} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

test("the stream's state, and a banner while it is down", () => {
  show();
  const state = screen.getByTestId("stream-state");
  expect(state).toHaveTextContent("connecting");
  act(() => FakeEventSource.last!.send(overview));
  expect(state).toHaveTextContent(`live · polling every ${overview.poll_interval_ms} ms`);
  expect(screen.queryByTestId("stream-banner")).toBeNull();

  act(() => FakeEventSource.last!.onerror?.());
  expect(state).toHaveTextContent("reconnecting");
  const banner = screen.getByTestId("stream-banner");
  expect(banner).toHaveTextContent("Live stream disconnected");
  expect(banner).toHaveTextContent(clock(overview.generated_at));

  act(() => FakeEventSource.last!.send(overview));
  expect(screen.queryByTestId("stream-banner")).toBeNull();
});

test("dark by default; the light theme is saved for the next visit", () => {
  const first = show();
  expect(document.documentElement.dataset.theme).toBe("dark");
  fireEvent.click(screen.getByRole("button", { name: "Switch to light theme" }));
  expect(document.documentElement.dataset.theme).toBe("light");
  expect(localStorage.getItem(themeKey)).toBe("light");
  first.unmount();

  show();
  expect(document.documentElement.dataset.theme).toBe("light");
  fireEvent.click(screen.getByRole("button", { name: "Switch to dark theme" }));
  expect(localStorage.getItem(themeKey)).toBe("dark");
});
