import { useState, useMemo } from 'react';

export default function HoldingsTable({ holdings, latestDate }) {
  const [sortConfig, setSortConfig] = useState({ key: 'weight', direction: 'desc' });

  const sortedHoldings = useMemo(() => {
    if (!holdings || holdings.length === 0) return [];
    
    const sorted = [...holdings].sort((a, b) => {
      if (a[sortConfig.key] < b[sortConfig.key]) {
        return sortConfig.direction === 'asc' ? -1 : 1;
      }
      if (a[sortConfig.key] > b[sortConfig.key]) {
        return sortConfig.direction === 'asc' ? 1 : -1;
      }
      return 0;
    });
    
    return sorted;
  }, [holdings, sortConfig]);

  const handleSort = (key) => {
    setSortConfig((current) => ({
      key,
      direction: current.key === key && current.direction === 'desc' ? 'asc' : 'desc',
    }));
  };

  const getSortIcon = (key) => {
    if (sortConfig.key !== key) {
      return <span className="text-gray-300 dark:text-gray-600 ml-1">↕</span>;
    }
    return sortConfig.direction === 'desc' 
      ? <span className="text-blue-600 dark:text-blue-400 ml-1">↓</span>
      : <span className="text-blue-600 dark:text-blue-400 ml-1">↑</span>;
  };

  const formatPercent = (value) => `${(value * 100).toFixed(2)}%`;
  const formatCurrency = (value) => `$${value.toFixed(2)}`;

  if (!holdings || holdings.length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <p className="text-gray-500 dark:text-gray-400 text-center">Upload an ETF file to view holdings</p>
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white">ETF Holdings</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400">As of {latestDate}</p>
      </div>
      
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-50 dark:bg-gray-700">
            <tr>
              <th 
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600"
                onClick={() => handleSort('name')}
              >
                <div className="flex items-center">
                  Constituent {getSortIcon('name')}
                </div>
              </th>
              <th 
                className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600"
                onClick={() => handleSort('weight')}
              >
                <div className="flex items-center justify-end">
                  Weight {getSortIcon('weight')}
                </div>
              </th>
              <th 
                className="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600"
                onClick={() => handleSort('latest_price')}
              >
                <div className="flex items-center justify-end">
                  Latest Price {getSortIcon('latest_price')}
                </div>
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
            {sortedHoldings.map((holding, index) => (
              <tr 
                key={holding.name}
                className={index % 2 === 0 ? 'bg-white dark:bg-gray-800' : 'bg-gray-50 dark:bg-gray-750 dark:bg-gray-800/50'}
              >
                <td className="px-6 py-4 whitespace-nowrap">
                  <span className="inline-flex items-center justify-center w-8 h-8 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 font-semibold text-sm">
                    {holding.name}
                  </span>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-right text-sm text-gray-900 dark:text-gray-100">
                  {formatPercent(holding.weight)}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium text-gray-900 dark:text-gray-100">
                  {formatCurrency(holding.latest_price)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      <div className="px-6 py-3 bg-gray-50 dark:bg-gray-700 border-t border-gray-200 dark:border-gray-600">
        <p className="text-sm text-gray-500 dark:text-gray-400">
          Total: {holdings.length} constituents • 
          Weight Sum: {formatPercent(holdings.reduce((sum, h) => sum + h.weight, 0))}
        </p>
      </div>
    </div>
  );
}
