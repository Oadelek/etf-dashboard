import { useState, useEffect } from 'react';
import axios from 'axios';
import StatsCard from '../components/StatsCard';
import {
  PieChart, Pie, Cell, ResponsiveContainer, Tooltip,
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
} from 'recharts';

const SECTOR_COLORS = [
  '#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6',
  '#06b6d4', '#ec4899', '#f97316', '#14b8a6', '#6366f1',
  '#84cc16',
];

export default function OverviewPage({ darkMode }) {
  const [stats, setStats] = useState(null);
  const [sectors, setSectors] = useState([]);
  const [volumeLeaders, setVolumeLeaders] = useState([]);
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchAll = async () => {
      try {
        const [statsRes, sectorRes, volumeRes, pipelineRes] = await Promise.all([
          axios.get('/api/v2/db-stats'),
          axios.get('/api/v2/analytics/sector-breakdown'),
          axios.get('/api/v2/analytics/volume-leaders?n=8'),
          axios.get('/api/v2/ingestion/status'),
        ]);
        setStats(statsRes.data);
        setSectors(sectorRes.data);
        setVolumeLeaders(volumeRes.data);
        setPipelineStatus(pipelineRes.data);
      } catch (err) {
        console.error('Failed to load overview data', err);
      } finally {
        setLoading(false);
      }
    };
    fetchAll();
  }, []);

  const gridColor = darkMode ? '#374151' : '#e5e7eb';
  const textColor = darkMode ? '#9ca3af' : '#6b7280';

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
      </div>
    );
  }

  const formatVolume = (v) => {
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`;
    return v;
  };

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Dashboard Overview</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Real-time view of your ETF data warehouse
        </p>
      </div>

      {/* Stats cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatsCard
          title="Total Records"
          value={stats?.total_price_records?.toLocaleString() ?? '—'}
          subtitle="OHLCV price rows"
          color="blue"
        />
        <StatsCard
          title="Constituents"
          value={stats?.total_constituents ?? '—'}
          subtitle="S&P 500 tickers tracked"
          color="green"
        />
        <StatsCard
          title="Trading Days"
          value={pipelineStatus?.total_trading_days?.toLocaleString() ?? '—'}
          subtitle={`${pipelineStatus?.date_range?.min ?? '?'} → ${pipelineStatus?.date_range?.max ?? '?'}`}
          color="purple"
        />
        <StatsCard
          title="Pipeline Health"
          value={pipelineStatus?.pipeline_healthy ? 'Healthy' : 'Pending'}
          subtitle={pipelineStatus?.pipeline_healthy
            ? `${pipelineStatus.processed_files_count} batches processed`
            : `${pipelineStatus?.landing_zone?.pending_files} files pending`}
          color={pipelineStatus?.pipeline_healthy ? 'green' : 'amber'}
          trend={pipelineStatus?.pipeline_healthy
            ? { positive: true, label: 'All clear' }
            : { positive: false, label: `${pipelineStatus?.landing_zone?.pending_files} pending` }}
        />
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Sector breakdown pie */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
          <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-1">Sector Breakdown</h2>
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">Constituents grouped by GICS sector</p>
          {sectors.length > 0 ? (
            <div className="flex flex-col lg:flex-row items-center gap-4">
              <div className="w-full lg:w-1/2 h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={sectors}
                      dataKey="ticker_count"
                      nameKey="sector"
                      cx="50%"
                      cy="50%"
                      innerRadius={50}
                      outerRadius={90}
                      strokeWidth={2}
                      stroke={darkMode ? '#1f2937' : '#ffffff'}
                    >
                      {sectors.map((_, i) => (
                        <Cell key={i} fill={SECTOR_COLORS[i % SECTOR_COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        backgroundColor: darkMode ? '#1f2937' : '#fff',
                        border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                        borderRadius: '8px',
                        fontSize: '13px',
                      }}
                      formatter={(value, name) => [`${value} tickers`, name]}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="w-full lg:w-1/2 space-y-2">
                {sectors.map((s, i) => (
                  <div key={s.sector} className="flex items-center justify-between text-sm">
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: SECTOR_COLORS[i % SECTOR_COLORS.length] }} />
                      <span className="text-gray-700 dark:text-gray-300 truncate">{s.sector}</span>
                    </div>
                    <span className="text-gray-500 dark:text-gray-400 font-medium tabular-nums">{s.ticker_count}</span>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <p className="text-gray-400 text-center py-8">No sector data available</p>
          )}
        </div>

        {/* Volume leaders bar chart */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
          <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-1">Volume Leaders</h2>
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">Top 8 tickers by average daily trading volume</p>
          {volumeLeaders.length > 0 ? (
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={volumeLeaders} margin={{ top: 5, right: 20, left: 5, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridColor} vertical={false} />
                  <XAxis dataKey="ticker" tick={{ fontSize: 11, fill: textColor }} stroke={textColor} />
                  <YAxis tickFormatter={formatVolume} tick={{ fontSize: 11, fill: textColor }} stroke={textColor} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: darkMode ? '#1f2937' : '#fff',
                      border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                      borderRadius: '8px',
                    }}
                    formatter={(value) => [formatVolume(value), 'Avg Volume']}
                  />
                  <Bar dataKey="avg_volume" radius={[4, 4, 0, 0]}>
                    {volumeLeaders.map((_, i) => (
                      <Cell key={i} fill={SECTOR_COLORS[i % SECTOR_COLORS.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <p className="text-gray-400 text-center py-8">No volume data available</p>
          )}
        </div>
      </div>

      {/* ETFs + data freshness */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* ETF count card */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
          <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-3">Loaded ETFs</h2>
          <div className="text-4xl font-bold text-blue-600 dark:text-blue-400">{stats?.total_etf_uploads ?? 0}</div>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            {stats?.total_holdings ?? 0} total holdings across all ETFs
          </p>
          <div className="mt-4 pt-4 border-t border-gray-100 dark:border-gray-700">
            <a href="/etfs" className="text-sm text-blue-600 dark:text-blue-400 hover:underline font-medium">
              View ETF Explorer →
            </a>
          </div>
        </div>

        {/* Data freshness */}
        <div className="lg:col-span-2 bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
          <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-3">Data Freshness</h2>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">High Watermark</p>
              <p className="text-lg font-semibold text-gray-900 dark:text-white mt-1">
                {pipelineStatus?.high_watermark ?? 'N/A'}
              </p>
            </div>
            <div>
              <p className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">Date Range</p>
              <p className="text-lg font-semibold text-gray-900 dark:text-white mt-1">
                {pipelineStatus?.date_range?.min ?? '?'} → {pipelineStatus?.date_range?.max ?? '?'}
              </p>
            </div>
            <div>
              <p className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">Pending Files</p>
              <p className="text-lg font-semibold text-gray-900 dark:text-white mt-1">
                {pipelineStatus?.landing_zone?.pending_files ?? 0}
              </p>
            </div>
            <div>
              <p className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wider">Lagging Tickers</p>
              <p className="text-lg font-semibold text-gray-900 dark:text-white mt-1">
                {pipelineStatus?.lagging_tickers?.length ?? 0}
              </p>
            </div>
          </div>
          {pipelineStatus?.lagging_tickers?.length > 0 && (
            <div className="mt-3 p-3 bg-amber-50 dark:bg-amber-900/20 rounded-lg">
              <p className="text-xs text-amber-700 dark:text-amber-400">
                ⚠ Lagging: {pipelineStatus.lagging_tickers.map(t => t.ticker).join(', ')}
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
