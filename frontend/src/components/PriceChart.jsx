import { useState, useMemo } from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Brush,
} from 'recharts';

const TIME_RANGES = [
  { label: '1W', days: 7 },
  { label: '1M', days: 30 },
  { label: '3M', days: 90 },
  { label: 'ALL', days: null },
];

export default function PriceChart({ data, darkMode }) {
  const [selectedRange, setSelectedRange] = useState('ALL');

  if (!data || data.length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 h-96 flex items-center justify-center">
        <p className="text-gray-500 dark:text-gray-400">Upload an ETF file to view price chart</p>
      </div>
    );
  }

  // Filter data based on selected time range
  const filteredData = useMemo(() => {
    const range = TIME_RANGES.find(r => r.label === selectedRange);
    if (!range || range.days === null) {
      return data;
    }
    return data.slice(-range.days);
  }, [data, selectedRange]);

  // Determine if price went up or down (compare first to last in filtered range)
  const firstPrice = filteredData[0]?.price || 0;
  const lastPrice = filteredData[filteredData.length - 1]?.price || 0;
  const isPositive = lastPrice >= firstPrice;
  const priceChange = lastPrice - firstPrice;
  const percentChange = firstPrice > 0 ? ((priceChange / firstPrice) * 100) : 0;

  // Colors based on performance
  const lineColor = isPositive ? '#22c55e' : '#ef4444';
  const gradientId = isPositive ? 'priceGradientGreen' : 'priceGradientRed';

  // Theme colors
  const gridColor = darkMode ? '#374151' : '#e5e7eb';
  const textColor = darkMode ? '#9ca3af' : '#6b7280';

  // Format x-axis
  const formatXAxis = (dateStr) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  // Custom tooltip
  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const date = new Date(label);
      const formattedDate = date.toLocaleDateString('en-US', { 
        month: 'long', 
        day: 'numeric', 
        year: 'numeric' 
      });
      const colorClass = isPositive 
        ? 'text-green-600 dark:text-green-400' 
        : 'text-red-600 dark:text-red-400';
      return (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 shadow-lg rounded p-3">
          <p className="text-sm text-gray-600 dark:text-gray-400">{formattedDate}</p>
          <p className={`text-lg font-semibold ${colorClass}`}>
            ${payload[0].value.toFixed(2)}
          </p>
        </div>
      );
    }
    return null;
  };

  // Calculate Y-axis domain with padding
  const prices = filteredData.map(d => d.price);
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  const padding = (maxPrice - minPrice) * 0.1;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-lg font-semibold text-gray-800 dark:text-white">ETF Price History</h2>
              <span className="text-gray-400 dark:text-gray-500">•</span>
              <span className="text-gray-500 dark:text-gray-400 whitespace-nowrap">
                {new Date(filteredData[filteredData.length - 1]?.date).toLocaleDateString('en-US', { 
                  month: 'long', 
                  day: 'numeric', 
                  year: 'numeric' 
                })}
              </span>
            </div>
            <div className="flex items-center gap-3 mt-2">
              <span className="text-2xl font-bold text-gray-900 dark:text-white">
                ${lastPrice.toFixed(2)}
              </span>
              <span className={`text-sm font-medium px-2 py-0.5 rounded ${
                isPositive 
                  ? 'text-green-700 dark:text-green-400 bg-green-100 dark:bg-green-900/30' 
                  : 'text-red-700 dark:text-red-400 bg-red-100 dark:bg-red-900/30'
              }`}>
                {isPositive ? '▲' : '▼'} {isPositive ? '+' : ''}{priceChange.toFixed(2)} ({isPositive ? '+' : ''}{percentChange.toFixed(2)}%)
              </span>
            </div>
          </div>
          
          {/* Time Range Selector */}
          <div className="flex gap-2">
            {TIME_RANGES.map(range => (
              <button
                key={range.label}
                onClick={() => setSelectedRange(range.label)}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${
                  selectedRange === range.label
                    ? 'bg-blue-600 text-white shadow-md'
                    : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-600'
                }`}
              >
                {range.label}
              </button>
            ))}
          </div>
        </div>
      </div>
      
      <div className="p-4 h-96">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={filteredData}
            margin={{ top: 10, right: 30, left: 10, bottom: 30 }}
          >
            <defs>
              <linearGradient id="priceGradientGreen" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#22c55e" stopOpacity={0.3} />
                <stop offset="100%" stopColor="#22c55e" stopOpacity={0.02} />
              </linearGradient>
              <linearGradient id="priceGradientRed" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#ef4444" stopOpacity={0.3} />
                <stop offset="100%" stopColor="#ef4444" stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke={gridColor} vertical={false} />
            <XAxis 
              dataKey="date" 
              tickFormatter={formatXAxis}
              tick={{ fontSize: 12, fill: textColor }}
              stroke={textColor}
              axisLine={false}
              tickLine={false}
              interval="preserveStartEnd"
              minTickGap={50}
            />
            <YAxis 
              domain={[minPrice - padding, maxPrice + padding]}
              tickFormatter={(value) => `$${value.toFixed(0)}`}
              tick={{ fontSize: 12, fill: textColor }}
              stroke={textColor}
              axisLine={false}
              tickLine={false}
              width={60}
              tickCount={5}
            />
            <Tooltip content={<CustomTooltip />} />
            <Area
              type="monotone"
              dataKey="price"
              stroke={lineColor}
              strokeWidth={2}
              fill={`url(#${gradientId})`}
              dot={false}
              activeDot={{ r: 5, fill: lineColor, strokeWidth: 0 }}
            />
            <Brush
              dataKey="date"
              height={25}
              stroke={lineColor}
              tickFormatter={formatXAxis}
              fill={darkMode ? '#1f2937' : '#f9fafb'}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
