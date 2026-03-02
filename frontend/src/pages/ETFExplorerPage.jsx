import { useState, useEffect } from 'react';
import axios from 'axios';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, BarChart, Bar, Cell,
} from 'recharts';

const COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6',
  '#06b6d4', '#ec4899', '#f97316', '#14b8a6', '#6366f1'];

const TIME_RANGES = [
  { label: '1M', days: 22 },
  { label: '3M', days: 66 },
  { label: '6M', days: 132 },
  { label: '1Y', days: 252 },
  { label: 'ALL', days: null },
];

export default function ETFExplorerPage({ darkMode }) {
  const [etfs, setEtfs] = useState([]);
  const [selectedEtf, setSelectedEtf] = useState(null);
  const [holdings, setHoldings] = useState([]);
  const [prices, setPrices] = useState([]);
  const [topHoldings, setTopHoldings] = useState([]);
  const [bestWorst, setBestWorst] = useState(null);
  const [selectedRange, setSelectedRange] = useState('ALL');
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);

  const gridColor = darkMode ? '#374151' : '#e5e7eb';
  const textColor = darkMode ? '#9ca3af' : '#6b7280';

  useEffect(() => {
    axios.get('/api/v2/etfs').then(res => {
      setEtfs(res.data);
      if (res.data.length > 0) {
        setSelectedEtf(res.data[0]);
      }
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!selectedEtf) return;
    setDetailLoading(true);
    Promise.all([
      axios.get(`/api/v2/etfs/${selectedEtf.id}/holdings`),
      axios.get(`/api/v2/etfs/${selectedEtf.id}/prices`),
      axios.get(`/api/v2/etfs/${selectedEtf.id}/top-holdings?n=10`),
      axios.get(`/api/v2/etfs/${selectedEtf.id}/best-worst-days?n=5`),
    ]).then(([holdRes, priceRes, topRes, bwRes]) => {
      setHoldings(holdRes.data.holdings ?? []);
      setPrices(priceRes.data.prices ?? []);
      setTopHoldings(topRes.data.top_holdings ?? []);
      setBestWorst(bwRes.data ?? null);
    }).catch(console.error)
      .finally(() => setDetailLoading(false));
  }, [selectedEtf]);

  const filteredPrices = (() => {
    const range = TIME_RANGES.find(r => r.label === selectedRange);
    if (!range || !range.days) return prices;
    return prices.slice(-range.days);
  })();

  const firstPrice = filteredPrices[0]?.price ?? 0;
  const lastPrice = filteredPrices[filteredPrices.length - 1]?.price ?? 0;
  const isPositive = lastPrice >= firstPrice;
  const pctChange = firstPrice > 0 ? ((lastPrice - firstPrice) / firstPrice * 100) : 0;
  const lineColor = isPositive ? '#22c55e' : '#ef4444';

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    );
  }

  if (etfs.length === 0) {
    return (
      <div className="text-center py-16">
        <svg className="mx-auto w-16 h-16 text-gray-300 dark:text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
        </svg>
        <h3 className="mt-4 text-lg font-medium text-gray-900 dark:text-white">No ETFs loaded</h3>
        <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">Run the ETL pipeline to populate ETF data, or upload a CSV on the Upload page.</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">ETF Explorer</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Select an ETF to view its holdings, price history, and performance
        </p>
      </div>

      {/* ETF selector tabs */}
      <div className="flex flex-wrap gap-2">
        {etfs.map(etf => (
          <button
            key={etf.id}
            onClick={() => { setSelectedEtf(etf); setSelectedRange('ALL'); }}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              selectedEtf?.id === etf.id
                ? 'bg-blue-600 text-white shadow-md shadow-blue-500/25'
                : 'bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-700 hover:border-blue-300 dark:hover:border-blue-600'
            }`}
          >
            {etf.filename}
            <span className="ml-2 text-xs opacity-75">{etf.num_constituents ?? ''} holdings</span>
          </button>
        ))}
      </div>

      {detailLoading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
        </div>
      ) : (
        <>
          {/* Price chart */}
          <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 overflow-hidden">
            <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-700/50">
              <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
                <div>
                  <h2 className="text-lg font-semibold text-gray-800 dark:text-white">{selectedEtf?.filename} Price</h2>
                  <div className="flex items-center gap-3 mt-1">
                    <span className="text-2xl font-bold text-gray-900 dark:text-white">
                      ${lastPrice.toFixed(2)}
                    </span>
                    <span className={`text-sm font-medium px-2 py-0.5 rounded ${
                      isPositive
                        ? 'text-emerald-700 dark:text-emerald-400 bg-emerald-100 dark:bg-emerald-900/30'
                        : 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30'
                    }`}>
                      {isPositive ? '▲' : '▼'} {isPositive ? '+' : ''}{pctChange.toFixed(2)}%
                    </span>
                  </div>
                </div>
                <div className="flex gap-1.5">
                  {TIME_RANGES.map(r => (
                    <button
                      key={r.label}
                      onClick={() => setSelectedRange(r.label)}
                      className={`px-3 py-1.5 text-xs font-medium rounded-lg transition-all ${
                        selectedRange === r.label
                          ? 'bg-blue-600 text-white'
                          : 'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-600'
                      }`}
                    >
                      {r.label}
                    </button>
                  ))}
                </div>
              </div>
            </div>
            <div className="p-4 h-80">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={filteredPrices} margin={{ top: 10, right: 20, left: 10, bottom: 0 }}>
                  <defs>
                    <linearGradient id="etfGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={lineColor} stopOpacity={0.3} />
                      <stop offset="100%" stopColor={lineColor} stopOpacity={0.02} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridColor} vertical={false} />
                  <XAxis
                    dataKey="date"
                    tickFormatter={d => new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                    tick={{ fontSize: 11, fill: textColor }}
                    stroke={textColor}
                    axisLine={false}
                    tickLine={false}
                    minTickGap={50}
                  />
                  <YAxis
                    tickFormatter={v => `$${v.toFixed(0)}`}
                    tick={{ fontSize: 11, fill: textColor }}
                    stroke={textColor}
                    axisLine={false}
                    tickLine={false}
                    width={55}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: darkMode ? '#1f2937' : '#fff',
                      border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                      borderRadius: '8px',
                    }}
                    formatter={v => [`$${v.toFixed(2)}`, 'ETF Price']}
                    labelFormatter={d => new Date(d).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}
                  />
                  <Area type="monotone" dataKey="price" stroke={lineColor} strokeWidth={2} fill="url(#etfGradient)" dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Holdings + Top holdings */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Holdings table */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 overflow-hidden flex flex-col max-h-[500px]">
              <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-700/50 flex-shrink-0">
                <h2 className="text-lg font-semibold text-gray-800 dark:text-white">Holdings</h2>
                <p className="text-xs text-gray-500 dark:text-gray-400">{holdings.length} constituents</p>
              </div>
              <div className="overflow-auto flex-1">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 dark:bg-gray-700/50 sticky top-0">
                    <tr>
                      <th className="px-4 py-2.5 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Ticker</th>
                      <th className="px-4 py-2.5 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Sector</th>
                      <th className="px-4 py-2.5 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Weight</th>
                      <th className="px-4 py-2.5 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Price</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
                    {holdings.map((h, i) => (
                      <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/30">
                        <td className="px-4 py-2.5 font-medium text-gray-900 dark:text-white">{h.name}</td>
                        <td className="px-4 py-2.5 text-gray-500 dark:text-gray-400 truncate max-w-[120px]">{h.sector ?? '—'}</td>
                        <td className="px-4 py-2.5 text-right text-gray-700 dark:text-gray-300">{(h.weight * 100).toFixed(2)}%</td>
                        <td className="px-4 py-2.5 text-right font-medium text-gray-900 dark:text-white">${h.latest_price?.toFixed(2) ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Top holdings bar */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5 flex flex-col max-h-[500px]">
              <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-1">Top Holdings by Value</h2>
              <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">Weight × Latest Price</p>
              <div className="flex-1 min-h-0">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={topHoldings.slice(0, 10)} layout="vertical" margin={{ top: 0, right: 20, left: 5, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={gridColor} horizontal={true} vertical={false} />
                    <XAxis type="number" tickFormatter={v => `$${v.toFixed(0)}`} tick={{ fontSize: 11, fill: textColor }} />
                    <YAxis type="category" dataKey="name" tick={{ fontSize: 12, fill: textColor, fontWeight: 500 }} width={45} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: darkMode ? '#1f2937' : '#fff',
                        border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                        borderRadius: '8px',
                      }}
                      formatter={v => [`$${v.toFixed(2)}`, 'Holding Value']}
                    />
                    <Bar dataKey="holding_value" radius={[0, 4, 4, 0]}>
                      {topHoldings.slice(0, 10).map((_, i) => (
                        <Cell key={i} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>

          {/* Best / worst days */}
          {bestWorst && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <DaysList title="🟢 Best Days" days={bestWorst.best_days} positive />
              <DaysList title="🔴 Worst Days" days={bestWorst.worst_days} />
            </div>
          )}
        </>
      )}
    </div>
  );
}

function DaysList({ title, days, positive = false }) {
  if (!days || days.length === 0) return null;
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
      <h3 className="text-lg font-semibold text-gray-800 dark:text-white mb-3">{title}</h3>
      <div className="space-y-2">
        {days.map((d, i) => (
          <div key={i} className="flex items-center justify-between py-2 px-3 rounded-lg bg-gray-50 dark:bg-gray-700/30">
            <span className="text-sm text-gray-600 dark:text-gray-300">{d.date}</span>
            <div className="flex items-center gap-3">
              <span className="text-sm text-gray-500 dark:text-gray-400">${d.etf_price?.toFixed(2)}</span>
              <span className={`text-sm font-semibold px-2 py-0.5 rounded ${
                positive
                  ? 'text-emerald-700 dark:text-emerald-400 bg-emerald-100 dark:bg-emerald-900/30'
                  : 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30'
              }`}>
                {d.return_pct > 0 ? '+' : ''}{d.return_pct?.toFixed(2)}%
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
