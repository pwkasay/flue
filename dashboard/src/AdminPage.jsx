import { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { Card, CardTitle, LiveDot, Skeleton, timeAgo, fetchJSON } from './shared';

const REFRESH_MS = 30 * 1000; // 30 seconds

function ConnectorDot({ status }) {
  const color = status === 'active' ? '#22c55e' : status === 'stale' ? '#facc15' : '#ef4444';
  return <LiveDot color={color} />;
}

function ConnectorLabel({ status }) {
  const labels = { active: 'Active', stale: 'Stale', inactive: 'Inactive', available: 'Available' };
  const colors = { active: 'text-green-400', stale: 'text-yellow-400', inactive: 'text-red-400', available: 'text-gray-400' };
  return (
    <span className={`text-xs font-medium font-display uppercase tracking-wider ${colors[status] || 'text-gray-500'}`}>
      {labels[status] || status}
    </span>
  );
}

function AdminPage() {
  const [status, setStatus] = useState(null);
  const [events, setEvents] = useState(null);
  const [tick, setTick] = useState(0);

  const fetchData = useCallback(async () => {
    const [statusData, eventsData] = await Promise.all([
      fetchJSON('/admin/status'),
      fetchJSON('/admin/events?limit=20&event_type=validation_failure'),
    ]);

    // Also fetch persist failures and merge
    const persistEvents = await fetchJSON('/admin/events?limit=20&event_type=persist_failure');

    if (statusData) setStatus(statusData);
    if (eventsData) {
      const allFailures = [
        ...(eventsData.events || []),
        ...(persistEvents?.events || []),
      ].sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp)).slice(0, 20);
      setEvents(allFailures);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, REFRESH_MS);
    return () => clearInterval(interval);
  }, [fetchData]);

  // Tick for relative time updates
  useEffect(() => {
    const t = setInterval(() => setTick(p => p + 1), 10000);
    return () => clearInterval(t);
  }, []);

  const ingestion = status?.ingestion;
  const connectors = status?.connectors;

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="border-b border-carbon-700/30 bg-carbon-950/80 backdrop-blur-md sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link to="/" className="flex items-center gap-2 hover:opacity-80 transition-opacity">
              <svg viewBox="0 0 24 24" className="w-5 h-5 text-canary-500" fill="currentColor">
                <path d="M12 2C10 2 8.5 3.5 8 5.5C7 5 5.5 5 4.5 6C3 7.5 3 10 4 12C3 13 2 15 3 17C4 19 6 20 8 20C8 21 9 22 11 22C13 22 14 21.5 15 20.5C17 20.5 19 19 20 17C21 15 20.5 13 19.5 11.5C21 9.5 21 7 19.5 5.5C18 4 16 4 14.5 4.5C13.5 3 12.5 2 12 2Z"/>
              </svg>
              <span className="font-display font-semibold text-lg tracking-tight">
                <span className="text-canary-500">Canary</span>
              </span>
            </Link>
            <span className="text-[10px] font-data text-carbon-600 border border-carbon-700/50 rounded px-1.5 py-0.5 uppercase">
              Admin
            </span>
          </div>
          <div className="flex items-center gap-4 text-xs text-gray-500">
            <Link to="/" className="font-display text-gray-400 hover:text-canary-500 transition-colors">
              Dashboard
            </Link>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-5">

        {/* ── Connector Status Cards ── */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-5">

          {/* NYISO Connector */}
          <Card glowColor={connectors?.nyiso?.status === 'active' ? '#22c55e' : undefined}>
            <CardTitle icon="NYISO">Data Source</CardTitle>
            {connectors ? (
              <div className="space-y-3">
                <div className="flex items-center gap-3">
                  <ConnectorDot status={connectors.nyiso.status} />
                  <ConnectorLabel status={connectors.nyiso.status} />
                </div>
                <div className="space-y-1.5">
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-500 font-display">Last data</span>
                    <span className="font-data text-gray-300">
                      {connectors.nyiso.last_data_at ? timeAgo(connectors.nyiso.last_data_at) : 'Never'}
                    </span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-500 font-display">Records/hour</span>
                    <span className="font-data text-gray-300">{ingestion?.records_last_hour ?? '-'}</span>
                  </div>
                </div>
              </div>
            ) : (
              <div className="space-y-3">
                <Skeleton className="h-6 w-24" />
                <Skeleton className="h-8 w-full" />
              </div>
            )}
          </Card>

          {/* Weather Connector */}
          <Card>
            <CardTitle icon="Weather">Weather Source</CardTitle>
            {connectors ? (
              <div className="space-y-3">
                <div className="flex items-center gap-3">
                  <ConnectorDot status={connectors.weather?.status || 'inactive'} />
                  <ConnectorLabel status={connectors.weather?.status || 'inactive'} />
                </div>
                <div className="space-y-1.5">
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-500 font-display">Provider</span>
                    <span className="font-data text-gray-300">Open-Meteo</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-500 font-display">Used for</span>
                    <span className="font-data text-gray-300">Forecast corrections</span>
                  </div>
                </div>
              </div>
            ) : (
              <div className="space-y-3">
                <Skeleton className="h-6 w-24" />
                <Skeleton className="h-8 w-full" />
              </div>
            )}
          </Card>

          {/* Database Status */}
          <Card>
            <CardTitle icon="DB">Database</CardTitle>
            {ingestion ? (
              <div className="space-y-3">
                <div className="flex items-center gap-3">
                  <ConnectorDot status={ingestion.total_records > 0 ? 'active' : 'inactive'} />
                  <span className="text-xs font-medium font-display text-gray-300">PostgreSQL</span>
                </div>
                <div className="space-y-1.5">
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-500 font-display">Total records</span>
                    <span className="font-data text-gray-300">{ingestion.total_records?.toLocaleString() ?? '-'}</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-gray-500 font-display">Date range</span>
                    <span className="font-data text-gray-300 text-[10px]">
                      {ingestion.earliest ? new Date(ingestion.earliest).toLocaleDateString() : '-'}
                      {ingestion.earliest && ingestion.latest ? ' \u2192 ' : ''}
                      {ingestion.latest ? new Date(ingestion.latest).toLocaleDateString() : ''}
                    </span>
                  </div>
                </div>
              </div>
            ) : (
              <div className="space-y-3">
                <Skeleton className="h-6 w-24" />
                <Skeleton className="h-8 w-full" />
              </div>
            )}
          </Card>
        </div>

        {/* ── Data Freshness ── */}
        <Card>
          <CardTitle icon="Freshness">Data Freshness</CardTitle>
          {ingestion ? (
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
              <div>
                <p className="text-xs text-gray-500 font-display mb-1">Last Data Point</p>
                <p className="text-2xl font-bold font-data text-gray-200">
                  {ingestion.last_data_at ? timeAgo(ingestion.last_data_at) : 'Never'}
                </p>
                {ingestion.last_data_at && (
                  <p className="text-[10px] font-data text-gray-600 mt-1">
                    {new Date(ingestion.last_data_at).toLocaleString()}
                  </p>
                )}
              </div>
              <div>
                <p className="text-xs text-gray-500 font-display mb-1">Records Last Hour</p>
                <p className="text-2xl font-bold font-data text-gray-200">
                  {ingestion.records_last_hour}
                </p>
              </div>
              <div>
                <p className="text-xs text-gray-500 font-display mb-1">Errors Last Hour</p>
                <p className={`text-2xl font-bold font-data ${ingestion.errors_last_hour > 0 ? 'text-red-400' : 'text-gray-200'}`}>
                  {ingestion.errors_last_hour}
                </p>
              </div>
            </div>
          ) : (
            <div className="grid grid-cols-3 gap-6">
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
            </div>
          )}
        </Card>

        {/* ── Failures Table ── */}
        <Card>
          <div className="flex items-center justify-between mb-4">
            <CardTitle icon="Failures">Recent Failures</CardTitle>
            <span className="text-[10px] font-data text-gray-600">auto-refreshes every 30s</span>
          </div>
          {events !== null ? (
            events.length > 0 ? (
              <div className="overflow-x-auto -mx-6 px-6">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-carbon-700/30">
                      <th className="text-left py-2 pr-4 font-display font-medium text-gray-500 uppercase tracking-wider">Timestamp</th>
                      <th className="text-left py-2 pr-4 font-display font-medium text-gray-500 uppercase tracking-wider">Stage</th>
                      <th className="text-left py-2 pr-4 font-display font-medium text-gray-500 uppercase tracking-wider">Error Message</th>
                      <th className="text-right py-2 font-display font-medium text-gray-500 uppercase tracking-wider">Attempts</th>
                    </tr>
                  </thead>
                  <tbody>
                    {events.map((evt, i) => (
                      <tr key={i} className="border-b border-carbon-800/50 hover:bg-carbon-800/30 transition-colors">
                        <td className="py-2 pr-4 font-data text-gray-400 whitespace-nowrap">
                          {timeAgo(evt.timestamp)}
                        </td>
                        <td className="py-2 pr-4 font-data text-gray-300">
                          {evt.stage_name || evt.event_type}
                        </td>
                        <td className="py-2 pr-4 font-data text-red-400 max-w-md truncate">
                          {evt.message}
                        </td>
                        <td className="py-2 text-right font-data text-gray-500">
                          {evt.details?.attempts ?? '-'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center py-8">
                <p className="text-gray-500 font-display text-sm">No failures recorded</p>
                <p className="text-gray-600 font-display text-xs mt-1">Pipeline is running cleanly</p>
              </div>
            )
          ) : (
            <div className="space-y-2">
              {[...Array(3)].map((_, i) => <Skeleton key={i} className="h-8 w-full" />)}
            </div>
          )}
        </Card>

      </main>

      {/* Footer */}
      <footer className="border-t border-carbon-700/20 mt-8">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex items-center justify-between text-xs text-gray-600 font-display">
          <span>Canary Admin \u00b7 gridcarbon v0.1.0</span>
          <span>PostgreSQL \u00b7 weir pipeline</span>
        </div>
      </footer>
    </div>
  );
}

export default AdminPage;
