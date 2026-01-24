import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts';

// Color palette for the bars - distinct colors
const COLORS = ['#22c55e', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6'];

export default function TopHoldingsChart({ data, latestDate, darkMode }) {
  if (!data || data.length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 h-80 flex items-center justify-center">
        <p className="text-gray-500 dark:text-gray-400">Upload an ETF file to view top holdings</p>
      </div>
    );
  }

  // Theme colors
  const gridColor = darkMode ? '#374151' : '#e5e7eb';
  const textColor = darkMode ? '#9ca3af' : '#6b7280';

  // Custom tooltip
  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const item = payload[0].payload;
      return (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 shadow-lg rounded p-3">
          <p className="font-semibold text-gray-800 dark:text-white">Constituent {item.name}</p>
          <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">
            <p>Weight: {(item.weight * 100).toFixed(2)}%</p>
            <p>Price: ${item.latest_price.toFixed(2)}</p>
            <p className="font-medium text-blue-600 dark:text-blue-400 mt-1">
              Value: ${item.holding_value.toFixed(2)}
            </p>
          </div>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white">Top 5 Holdings by Value</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400">
          Holding value = Weight × Price • As of {latestDate}
        </p>
      </div>
      
      <div className="p-4 h-80">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={data}
            layout="vertical"
            margin={{ top: 10, right: 30, left: 40, bottom: 10 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke={gridColor} horizontal={true} vertical={false} />
            <XAxis 
              type="number"
              tickFormatter={(value) => `$${value.toFixed(0)}`}
              tick={{ fontSize: 12, fill: textColor }}
              stroke={textColor}
            />
            <YAxis 
              type="category"
              dataKey="name"
              tick={{ fontSize: 14, fontWeight: 500, fill: textColor }}
              stroke={textColor}
              width={30}
            />
            <Tooltip content={<CustomTooltip />} />
            <Bar 
              dataKey="holding_value" 
              radius={[0, 4, 4, 0]}
            >
              {data.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      
      <div className="px-6 py-3 bg-gray-50 dark:bg-gray-700 border-t border-gray-200 dark:border-gray-600">
        <div className="flex flex-wrap gap-4">
          {data.map((item, index) => (
            <div key={item.name} className="flex items-center gap-2">
              <div 
                className="w-3 h-3 rounded"
                style={{ backgroundColor: COLORS[index % COLORS.length] }}
              />
              <span className="text-sm text-gray-600 dark:text-gray-300">
                {item.name}: ${item.holding_value.toFixed(2)}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
