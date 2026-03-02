import { useState, useEffect } from 'react';
import axios from 'axios';
import StatsCard from '../components/StatsCard';

export default function PipelinePage({ darkMode }) {
  const [status, setStatus] = useState(null);
  const [watermarks, setWatermarks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState('ticker');
  const [sortDir, setSortDir] = useState('asc');

  useEffect(() => {
    Promise.all([
      axios.get('/api/v2/ingestion/status'),
      axios.get('/api/v2/ingestion/watermarks'),
    ]).then(([statusRes, wmRes]) => {
      setStatus(statusRes.data);
      setWatermarks(wmRes.data);
    }).catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir('asc');
    }
  };

  const sortedWatermarks = [...watermarks].sort((a, b) => {
    const va = a[sortKey] ?? '';
    const vb = b[sortKey] ?? '';
    if (typeof va === 'number') return sortDir === 'asc' ? va - vb : vb - va;
    return sortDir === 'asc' ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
  });

  const getSortIcon = (key) => {
    if (sortKey !== key) return <span className="text-gray-300 dark:text-gray-600 ml-1">↕</span>;
    return sortDir === 'asc'
      ? <span className="text-blue-500 ml-1">↑</span>
      : <span className="text-blue-500 ml-1">↓</span>;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Pipeline Monitor</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Data ingestion health, watermarks, and landing zone status
        </p>
      </div>

      {/* Health banner */}
      <div className={`rounded-xl p-4 border ${
        status?.pipeline_healthy
          ? 'bg-emerald-50 dark:bg-emerald-900/20 border-emerald-200 dark:border-emerald-800'
          : 'bg-amber-50 dark:bg-amber-900/20 border-amber-200 dark:border-amber-800'
      }`}>
        <div className="flex items-center gap-3">
          <div className={`w-3 h-3 rounded-full ${status?.pipeline_healthy ? 'bg-emerald-500 animate-pulse' : 'bg-amber-500 animate-pulse'}`} />
          <div>
            <p className={`font-semibold ${status?.pipeline_healthy ? 'text-emerald-800 dark:text-emerald-300' : 'text-amber-800 dark:text-amber-300'}`}>
              {status?.pipeline_healthy ? 'Pipeline Healthy — All Clear' : 'Pipeline Has Pending Files'}
            </p>
            <p className="text-sm text-gray-600 dark:text-gray-400 mt-0.5">{status?.freshness_note}</p>
          </div>
        </div>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatsCard
          title="Total Records"
          value={status?.total_records?.toLocaleString() ?? '—'}
          subtitle="OHLCV rows in database"
          color="blue"
        />
        <StatsCard
          title="Tickers Tracked"
          value={status?.total_tickers ?? '—'}
          subtitle="Unique constituents"
          color="green"
        />
        <StatsCard
          title="Trading Days"
          value={status?.total_trading_days?.toLocaleString() ?? '—'}
          subtitle={status?.date_range ? `${status.date_range.min} → ${status.date_range.max}` : ''}
          color="purple"
        />
        <StatsCard
          title="Processed Batches"
          value={status?.processed_files_count ?? 0}
          subtitle="Archived CSV files"
          color="indigo"
        />
      </div>

      {/* Landing zone + architecture row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Landing zone */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
          <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-3">Landing Zone</h2>
          <div className="space-y-3">
            <div className="flex items-center justify-between py-2 px-3 bg-gray-50 dark:bg-gray-700/30 rounded-lg">
              <span className="text-sm text-gray-600 dark:text-gray-300">Pending files</span>
              <span className={`text-sm font-bold ${
                status?.landing_zone?.pending_files > 0
                  ? 'text-amber-600 dark:text-amber-400'
                  : 'text-emerald-600 dark:text-emerald-400'
              }`}>
                {status?.landing_zone?.pending_files ?? 0}
              </span>
            </div>
            {status?.landing_zone?.pending_filenames?.length > 0 && (
              <div className="py-2 px-3 bg-amber-50 dark:bg-amber-900/20 rounded-lg">
                <p className="text-xs font-medium text-amber-700 dark:text-amber-400 mb-1">Pending Files:</p>
                {status.landing_zone.pending_filenames.map(f => (
                  <p key={f} className="text-xs text-amber-600 dark:text-amber-300 font-mono">{f}</p>
                ))}
              </div>
            )}
            <div className="flex items-center justify-between py-2 px-3 bg-gray-50 dark:bg-gray-700/30 rounded-lg">
              <span className="text-sm text-gray-600 dark:text-gray-300">High watermark</span>
              <span className="text-sm font-bold text-gray-900 dark:text-white">{status?.high_watermark ?? 'N/A'}</span>
            </div>
            <div className="flex items-center justify-between py-2 px-3 bg-gray-50 dark:bg-gray-700/30 rounded-lg">
              <span className="text-sm text-gray-600 dark:text-gray-300">Lagging tickers</span>
              <span className={`text-sm font-bold ${
                status?.lagging_tickers?.length > 0 ? 'text-amber-600' : 'text-emerald-600 dark:text-emerald-400'
              }`}>
                {status?.lagging_tickers?.length ?? 0}
              </span>
            </div>
          </div>
        </div>

        {/* Architecture diagram */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
          <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-3">Pipeline Architecture</h2>
          <div className="space-y-2 text-sm font-mono">
            {[
              { step: '1', label: 'DataProvider (simulator|yfinance|csv)', color: 'blue' },
              { step: '↓', label: '', color: '' },
              { step: '2', label: 'data/incoming/ — Landing Zone', color: 'amber' },
              { step: '↓', label: '', color: '' },
              { step: '3', label: 'Validate → Watermark Check', color: 'purple' },
              { step: '↓', label: '', color: '' },
              { step: '4', label: 'INSERT INTO prices (idempotent)', color: 'green' },
              { step: '↓', label: '', color: '' },
              { step: '5', label: 'Archive → data/processed/', color: 'indigo' },
            ].map((item, i) => (
              item.step === '↓' ? (
                <div key={i} className="text-center text-gray-400 dark:text-gray-500">↓</div>
              ) : (
                <div key={i} className={`flex items-center gap-3 py-2 px-3 rounded-lg bg-${item.color}-50 dark:bg-${item.color}-900/20`}>
                  <div className={`w-6 h-6 rounded-full bg-${item.color}-500 text-white text-xs flex items-center justify-center font-bold flex-shrink-0`}>
                    {item.step}
                  </div>
                  <span className={`text-${item.color}-700 dark:text-${item.color}-300`}>{item.label}</span>
                </div>
              )
            ))}
          </div>
        </div>
      </div>

      {/* Watermarks table */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-700/50">
          <h2 className="text-lg font-semibold text-gray-800 dark:text-white">Per-Ticker Watermarks</h2>
          <p className="text-xs text-gray-500 dark:text-gray-400">Latest date loaded for each ticker — detects partial loads or gaps</p>
        </div>
        <div className="overflow-auto max-h-[400px]">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 dark:bg-gray-700/50 sticky top-0">
              <tr>
                {[
                  { key: 'ticker', label: 'Ticker' },
                  { key: 'sector', label: 'Sector' },
                  { key: 'earliest_date', label: 'Earliest' },
                  { key: 'latest_date', label: 'Latest' },
                  { key: 'record_count', label: 'Records' },
                ].map(col => (
                  <th
                    key={col.key}
                    onClick={() => handleSort(col.key)}
                    className="px-4 py-2.5 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600"
                  >
                    <div className="flex items-center">{col.label} {getSortIcon(col.key)}</div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
              {sortedWatermarks.map((w, i) => {
                const isLagging = status?.lagging_tickers?.some(t => t.ticker === w.ticker);
                return (
                  <tr key={i} className={`hover:bg-gray-50 dark:hover:bg-gray-700/30 ${isLagging ? 'bg-amber-50/50 dark:bg-amber-900/10' : ''}`}>
                    <td className="px-4 py-2 font-medium text-gray-900 dark:text-white">
                      {w.ticker}
                      {isLagging && <span className="ml-1 text-amber-500 text-xs">⚠</span>}
                    </td>
                    <td className="px-4 py-2 text-gray-500 dark:text-gray-400 truncate max-w-[120px]">{w.sector ?? '—'}</td>
                    <td className="px-4 py-2 text-gray-500 dark:text-gray-400">{w.earliest_date}</td>
                    <td className="px-4 py-2 text-gray-900 dark:text-white font-medium">{w.latest_date}</td>
                    <td className="px-4 py-2 text-gray-500 dark:text-gray-400 tabular-nums">{w.record_count?.toLocaleString()}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
