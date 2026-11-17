import { Navigate, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { EventsPage } from "./pages/Events";
import { NodesPage } from "./pages/Nodes";
import { OverviewPage } from "./pages/Overview";
import { ShardsPage } from "./pages/Shards";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<OverviewPage />} />
        <Route path="shards" element={<ShardsPage />} />
        <Route path="nodes" element={<NodesPage />} />
        <Route path="events" element={<EventsPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
