import { useState, useEffect } from 'react';
import axios from 'axios';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ScatterChart, Scatter, ZAxis,
  ComposedChart, Bar, Area,
} from 'recharts';

const TICKER_SUGGESTIONS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'JPM', 'JNJ', 'V'];

export default function AnalyticsPage({ darkMode }) {
  const [activeTab, setActiveTab] = useState('moving-avg');

  const tabs = [
    { id: 'moving-avg', label: 'Moving Averages' },
    { id: 'ohlcv', label: 'OHLCV Data' },
    { id: 'correlation', label: 'Correlation' },
    { id: 'price-summary', label: 'Price Summary' },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Analytics</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          SQL-powered analytics: window functions, CTEs, self-JOINs, aggregations
        </p>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 p-1 bg-gray-100 dark:bg-gray-800 rounded-xl w-fit">
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${
              activeTab === tab.id
                ? 'bg-white dark:bg-gray-700 text-gray-900 dark:text-white shadow-sm'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === 'moving-avg' && <MovingAveragePanel darkMode={darkMode} />}
      {activeTab === 'ohlcv' && <OHLCVPanel darkMode={darkMode} />}
      {activeTab === 'correlation' && <CorrelationPanel darkMode={darkMode} />}
      {activeTab === 'price-summary' && <PriceSummaryPanel darkMode={darkMode} />}
    </div>
  );
}


function MovingAveragePanel({ darkMode }) {
  const [ticker, setTicker] = useState('AAPL');
  const [window, setWindow] = useState(20);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  const gridColor = darkMode ? '#374151' : '#e5e7eb';
  const textColor = darkMode ? '#9ca3af' : '#6b7280';

  const fetchData = () => {
    if (!ticker) return;
    setLoading(true);
    axios.get(`/api/v2/analytics/moving-average?ticker=${ticker}&window=${window}`)
      .then(res => setData(Array.isArray(res.data) ? res.data : (res.data?.prices ?? [])))
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { fetchData(); }, []);

  return (
    <div className="space-y-4">
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
        <div className="flex flex-wrap items-end gap-4 mb-6">
          <div>
            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Ticker</label>
            <div className="flex gap-2">
              <input
                value={ticker}
                onChange={e => setTicker(e.target.value.toUpperCase())}
                className="w-24 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-sm text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="AAPL"
              />
              <div className="flex gap-1">
                {TICKER_SUGGESTIONS.slice(0, 5).map(t => (
                  <button
                    key={t}
                    onClick={() => setTicker(t)}
                    className={`px-2 py-1 text-xs rounded-md transition-colors ${
                      ticker === t
                        ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300'
                        : 'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 hover:bg-gray-200'
                    }`}
                  >
                    {t}
                  </button>
                ))}
              </div>
            </div>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Window</label>
            <select
              value={window}
              onChange={e => setWindow(Number(e.target.value))}
              className="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-sm text-gray-900 dark:text-white"
            >
              {[5, 10, 20, 50].map(w => <option key={w} value={w}>{w}-day</option>)}
            </select>
          </div>
          <button
            onClick={fetchData}
            disabled={loading}
            className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {loading ? 'Loading...' : 'Analyze'}
          </button>
        </div>

        <p className="text-xs text-gray-400 dark:text-gray-500 mb-3">
          SQL: <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">AVG(close_price) OVER (ORDER BY date ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW)</code>
        </p>

        {data.length > 0 && (
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.slice(-120)} margin={{ top: 10, right: 20, left: 10, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={gridColor} vertical={false} />
                <XAxis
                  dataKey="date"
                  tickFormatter={d => new Date(d).toLocaleDateString('en-US', { month: 'short' })}
                  tick={{ fontSize: 11, fill: textColor }}
                  stroke={textColor} axisLine={false} tickLine={false} minTickGap={40}
                />
                <YAxis
                  tickFormatter={v => `$${v.toFixed(0)}`}
                  tick={{ fontSize: 11, fill: textColor }}
                  stroke={textColor} axisLine={false} tickLine={false} width={55}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: darkMode ? '#1f2937' : '#fff',
                    border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                    borderRadius: '8px', fontSize: '13px',
                  }}
                  formatter={(v, name) => [`$${v?.toFixed(2) ?? '—'}`, name]}
                  labelFormatter={d => new Date(d).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}
                />
                <Line type="monotone" dataKey="price" stroke="#3b82f6" strokeWidth={1.5} dot={false} name="Close" />
                <Line type="monotone" dataKey="moving_avg" stroke="#f59e0b" strokeWidth={2} dot={false} name={`${window}-day MA`} strokeDasharray="5 3" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </div>
  );
}


function OHLCVPanel({ darkMode }) {
  const [ticker, setTicker] = useState('AAPL');
  const [limit, setLimit] = useState(60);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  const gridColor = darkMode ? '#374151' : '#e5e7eb';
  const textColor = darkMode ? '#9ca3af' : '#6b7280';

  const fetchData = () => {
    setLoading(true);
    axios.get(`/api/v2/analytics/ohlcv?ticker=${ticker}&limit=${limit}`)
      .then(res => setData(Array.isArray(res.data) ? res.data : (res.data?.prices ?? [])))
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { fetchData(); }, []);

  const formatVolume = (v) => {
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`;
    return v;
  };

  return (
    <div className="space-y-4">
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
        <div className="flex flex-wrap items-end gap-4 mb-6">
          <div>
            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Ticker</label>
            <input
              value={ticker}
              onChange={e => setTicker(e.target.value.toUpperCase())}
              className="w-24 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-sm text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500"
              placeholder="AAPL"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Days</label>
            <select
              value={limit}
              onChange={e => setLimit(Number(e.target.value))}
              className="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-sm text-gray-900 dark:text-white"
            >
              {[30, 60, 120, 252, 500].map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
          <button onClick={fetchData} disabled={loading}
            className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50">
            {loading ? 'Loading...' : 'Fetch'}
          </button>
        </div>

        {data.length > 0 && (
          <>
            <div className="h-72 mb-4">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={data} margin={{ top: 10, right: 20, left: 10, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridColor} vertical={false} />
                  <XAxis dataKey="date" tickFormatter={d => new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                    tick={{ fontSize: 11, fill: textColor }} stroke={textColor} axisLine={false} minTickGap={50} />
                  <YAxis yAxisId="price" tickFormatter={v => `$${v.toFixed(0)}`}
                    tick={{ fontSize: 11, fill: textColor }} stroke={textColor} axisLine={false} width={55} />
                  <YAxis yAxisId="vol" orientation="right" tickFormatter={formatVolume}
                    tick={{ fontSize: 11, fill: textColor }} stroke={textColor} axisLine={false} width={55} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: darkMode ? '#1f2937' : '#fff',
                      border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                      borderRadius: '8px', fontSize: '12px',
                    }}
                    formatter={(v, name) => {
                      if (name === 'Volume') return [formatVolume(v), name];
                      return [`$${Number(v).toFixed(2)}`, name];
                    }}
                  />
                  <Bar yAxisId="vol" dataKey="volume" fill={darkMode ? '#374151' : '#e5e7eb'} opacity={0.5} name="Volume" />
                  <Line yAxisId="price" type="monotone" dataKey="high" stroke="#22c55e" strokeWidth={1} dot={false} name="High" />
                  <Line yAxisId="price" type="monotone" dataKey="low" stroke="#ef4444" strokeWidth={1} dot={false} name="Low" />
                  <Line yAxisId="price" type="monotone" dataKey="close" stroke="#3b82f6" strokeWidth={2} dot={false} name="Close" />
                </ComposedChart>
              </ResponsiveContainer>
            </div>

            {/* Data table */}
            <div className="overflow-auto max-h-64 rounded-lg border border-gray-200 dark:border-gray-700">
              <table className="w-full text-xs">
                <thead className="bg-gray-50 dark:bg-gray-700/50 sticky top-0">
                  <tr>
                    {['Date', 'Open', 'High', 'Low', 'Close', 'Volume'].map(h => (
                      <th key={h} className="px-3 py-2 text-left font-medium text-gray-500 dark:text-gray-400 uppercase">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
                  {data.slice(-30).reverse().map((r, i) => (
                    <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/30">
                      <td className="px-3 py-1.5 text-gray-600 dark:text-gray-300">{r.date}</td>
                      <td className="px-3 py-1.5 text-gray-900 dark:text-white">${r.open?.toFixed(2)}</td>
                      <td className="px-3 py-1.5 text-emerald-600 dark:text-emerald-400">${r.high?.toFixed(2)}</td>
                      <td className="px-3 py-1.5 text-red-600 dark:text-red-400">${r.low?.toFixed(2)}</td>
                      <td className="px-3 py-1.5 font-medium text-gray-900 dark:text-white">${r.close?.toFixed(2)}</td>
                      <td className="px-3 py-1.5 text-gray-500 dark:text-gray-400">{formatVolume(r.volume)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </div>
  );
}


function CorrelationPanel({ darkMode }) {
  const [tickerA, setTickerA] = useState('AAPL');
  const [tickerB, setTickerB] = useState('MSFT');
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  const gridColor = darkMode ? '#374151' : '#e5e7eb';
  const textColor = darkMode ? '#9ca3af' : '#6b7280';

  const fetchData = () => {
    setLoading(true);
    axios.get(`/api/v2/analytics/correlation?ticker_a=${tickerA}&ticker_b=${tickerB}`)
      .then(res => setData(Array.isArray(res.data) ? res.data : (res.data?.paired_prices ?? [])))
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { fetchData(); }, []);

  return (
    <div className="space-y-4">
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 p-5">
        <div className="flex flex-wrap items-end gap-4 mb-6">
          <div>
            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Ticker A</label>
            <input value={tickerA} onChange={e => setTickerA(e.target.value.toUpperCase())}
              className="w-24 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-sm text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Ticker B</label>
            <input value={tickerB} onChange={e => setTickerB(e.target.value.toUpperCase())}
              className="w-24 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-sm text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500" />
          </div>
          <button onClick={fetchData} disabled={loading}
            className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50">
            {loading ? 'Loading...' : 'Compare'}
          </button>
        </div>

        <p className="text-xs text-gray-400 dark:text-gray-500 mb-3">
          SQL: Self-JOIN on <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">prices</code> table pairing same dates for two tickers
        </p>

        {data.length > 0 && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Overlay line chart */}
            <div className="h-72">
              <h3 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Price Overlay</h3>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data.slice(-120)} margin={{ top: 5, right: 20, left: 10, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridColor} vertical={false} />
                  <XAxis dataKey="date" tickFormatter={d => new Date(d).toLocaleDateString('en-US', { month: 'short' })}
                    tick={{ fontSize: 11, fill: textColor }} axisLine={false} minTickGap={40} />
                  <YAxis tickFormatter={v => `$${v.toFixed(0)}`}
                    tick={{ fontSize: 11, fill: textColor }} axisLine={false} width={55} />
                  <Tooltip
                    contentStyle={{ backgroundColor: darkMode ? '#1f2937' : '#fff', border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`, borderRadius: '8px' }}
                    formatter={v => [`$${v?.toFixed(2)}`, '']}
                  />
                  <Line type="monotone" dataKey="price_a" stroke="#3b82f6" strokeWidth={2} dot={false} name={tickerA} />
                  <Line type="monotone" dataKey="price_b" stroke="#22c55e" strokeWidth={2} dot={false} name={tickerB} />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Scatter plot */}
            <div className="h-72">
              <h3 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Scatter (Correlation)</h3>
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
                  <XAxis dataKey="price_a" name={tickerA} tickFormatter={v => `$${v.toFixed(0)}`}
                    tick={{ fontSize: 11, fill: textColor }} />
                  <YAxis dataKey="price_b" name={tickerB} tickFormatter={v => `$${v.toFixed(0)}`}
                    tick={{ fontSize: 11, fill: textColor }} width={55} />
                  <ZAxis range={[20, 20]} />
                  <Tooltip
                    contentStyle={{ backgroundColor: darkMode ? '#1f2937' : '#fff', border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`, borderRadius: '8px' }}
                    formatter={v => [`$${v?.toFixed(2)}`]}
                  />
                  <Scatter data={data.slice(-252)} fill="#8b5cf6" opacity={0.6} />
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}


function PriceSummaryPanel({ darkMode }) {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    axios.get('/api/v2/analytics/price-summary')
      .then(res => setData(res.data ?? []))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="flex justify-center py-8"><div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500" /></div>;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 overflow-hidden">
      <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-700/50">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white">Price Summary</h2>
        <p className="text-xs text-gray-500 dark:text-gray-400">
          SQL: <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">GROUP BY ticker</code> with AVG, MIN, MAX, STDDEV aggregates
        </p>
      </div>
      <div className="overflow-auto max-h-[500px]">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 dark:bg-gray-700/50 sticky top-0">
            <tr>
              {['Ticker', 'Sector', 'Avg Price', 'Min', 'Max', 'Std Dev', 'Days'].map(h => (
                <th key={h} className="px-4 py-2.5 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase whitespace-nowrap">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
            {data.map((r, i) => (
              <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/30">
                <td className="px-4 py-2 font-medium text-gray-900 dark:text-white">{r.ticker}</td>
                <td className="px-4 py-2 text-gray-500 dark:text-gray-400 truncate max-w-[120px]">{r.sector ?? '—'}</td>
                <td className="px-4 py-2 text-gray-900 dark:text-white">${r.avg_price?.toFixed(2)}</td>
                <td className="px-4 py-2 text-red-600 dark:text-red-400">${r.min_price?.toFixed(2)}</td>
                <td className="px-4 py-2 text-emerald-600 dark:text-emerald-400">${r.max_price?.toFixed(2)}</td>
                <td className="px-4 py-2 text-gray-500 dark:text-gray-400">{r.std_dev?.toFixed(2) ?? '—'}</td>
                <td className="px-4 py-2 text-gray-500 dark:text-gray-400">{r.trading_days}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
