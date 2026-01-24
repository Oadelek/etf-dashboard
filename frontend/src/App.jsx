import { useState, useCallback, useEffect } from 'react';
import axios from 'axios';
import FileUpload from './components/FileUpload';
import HoldingsTable from './components/HoldingsTable';
import PriceChart from './components/PriceChart';
import TopHoldingsChart from './components/TopHoldingsChart';

export default function App() {
  // Dashboard state
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [filename, setFilename] = useState(null);
  const [darkMode, setDarkMode] = useState(() => {
    if (typeof window !== 'undefined') {
      return localStorage.getItem('darkMode') === 'true' ||
        window.matchMedia('(prefers-color-scheme: dark)').matches;
    }
    return false;
  });
  
  // Data from API
  const [holdings, setHoldings] = useState([]);
  const [etfPrices, setEtfPrices] = useState([]);
  const [topHoldings, setTopHoldings] = useState([]);
  const [latestDate, setLatestDate] = useState(null);
  const [summary, setSummary] = useState(null);

  // Apply dark mode class to document
  useEffect(() => {
    if (darkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    localStorage.setItem('darkMode', darkMode);
  }, [darkMode]);

  /**
   * Handle file upload
   */
  const handleUpload = useCallback(async (file) => {
    setIsLoading(true);
    setError(null);
    
    try {
      const formData = new FormData();
      formData.append('file', file);
      
      const response = await axios.post('/api/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });
      
      const data = response.data;
      
      setFilename(data.filename);
      setSummary(data.summary);
      setHoldings(data.holdings);
      setEtfPrices(data.etf_prices);
      setTopHoldings(data.top_holdings);
      setLatestDate(data.latest_date);
      
    } catch (err) {
      console.error('Upload error:', err);
      setError(
        err.response?.data?.detail || 
        'Failed to upload file. Please try again.'
      );
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Reset dashboard to initial state
   */
  const handleReset = useCallback(() => {
    setFilename(null);
    setSummary(null);
    setHoldings([]);
    setEtfPrices([]);
    setTopHoldings([]);
    setLatestDate(null);
    setError(null);
  }, []);

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 transition-colors duration-200">
      {/* Header */}
      <header className="bg-white dark:bg-gray-800 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 dark:text-white">ETF Dashboard</h1>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                View historical prices and holdings for uploaded ETFs
              </p>
            </div>
            <div className="flex items-center gap-4">
              {filename && (
                <>
                  <div className="text-right">
                    <p className="text-sm font-medium text-gray-900 dark:text-white">{filename}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      {summary?.constituents} constituents • 
                      {' '}{(summary?.total_weight * 100).toFixed(1)}% total weight
                    </p>
                  </div>
                  <button
                    onClick={handleReset}
                    className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    Upload New
                  </button>
                </>
              )}
              {/* Dark Mode Toggle */}
              <button
                onClick={() => setDarkMode(!darkMode)}
                className="p-2 rounded-lg bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
                aria-label="Toggle dark mode"
              >
                {darkMode ? (
                  <svg className="w-5 h-5 text-yellow-500" fill="currentColor" viewBox="0 0 20 20">
                    <path fillRule="evenodd" d="M10 2a1 1 0 011 1v1a1 1 0 11-2 0V3a1 1 0 011-1zm4 8a4 4 0 11-8 0 4 4 0 018 0zm-.464 4.95l.707.707a1 1 0 001.414-1.414l-.707-.707a1 1 0 00-1.414 1.414zm2.12-10.607a1 1 0 010 1.414l-.706.707a1 1 0 11-1.414-1.414l.707-.707a1 1 0 011.414 0zM17 11a1 1 0 100-2h-1a1 1 0 100 2h1zm-7 4a1 1 0 011 1v1a1 1 0 11-2 0v-1a1 1 0 011-1zM5.05 6.464A1 1 0 106.465 5.05l-.708-.707a1 1 0 00-1.414 1.414l.707.707zm1.414 8.486l-.707.707a1 1 0 01-1.414-1.414l.707-.707a1 1 0 011.414 1.414zM4 11a1 1 0 100-2H3a1 1 0 000 2h1z" clipRule="evenodd" />
                  </svg>
                ) : (
                  <svg className="w-5 h-5 text-gray-700" fill="currentColor" viewBox="0 0 20 20">
                    <path d="M17.293 13.293A8 8 0 016.707 2.707a8.001 8.001 0 1010.586 10.586z" />
                  </svg>
                )}
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 py-8 sm:px-6 lg:px-8">
        {/* Error Alert */}
        {error && (
          <div className="mb-6 p-4 bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 rounded-lg">
            <div className="flex items-center">
              <svg className="w-5 h-5 text-red-400 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <p className="text-sm text-red-700 dark:text-red-300">{error}</p>
            </div>
          </div>
        )}

        {/* Upload Section - shown when no file loaded */}
        {!filename && (
          <div className="max-w-xl mx-auto">
            <FileUpload onUpload={handleUpload} isLoading={isLoading} />
            <div className="mt-6 p-4 bg-blue-50 dark:bg-blue-900/30 rounded-lg">
              <h3 className="text-sm font-medium text-blue-800 dark:text-blue-300">How it works</h3>
              <ul className="mt-2 text-sm text-blue-700 dark:text-blue-400 list-disc list-inside space-y-1">
                <li>Upload an ETF weights CSV file</li>
                <li>View constituent holdings with their latest prices</li>
                <li>See the reconstructed ETF price over time</li>
                <li>Analyze top holdings by portfolio value</li>
              </ul>
            </div>
          </div>
        )}

        {/* Dashboard - shown when file is loaded */}
        {filename && (
          <div className="space-y-8">
            {/* Top row: Price Chart */}
            <PriceChart data={etfPrices} darkMode={darkMode} />
            
            {/* Bottom row: Table and Bar Chart side by side */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
              <HoldingsTable holdings={holdings} latestDate={latestDate} />
              <TopHoldingsChart data={topHoldings} latestDate={latestDate} darkMode={darkMode} />
            </div>
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="bg-white dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700 mt-auto">
        <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6 lg:px-8">
          <p className="text-center text-sm text-gray-500 dark:text-gray-400">
            ETF Dashboard - Built with React, FastAPI, and Recharts
          </p>
        </div>
      </footer>
    </div>
  );
}
