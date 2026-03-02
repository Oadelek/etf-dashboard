import { useState, useCallback } from 'react';
import axios from 'axios';
import FileUpload from '../components/FileUpload';
import HoldingsTable from '../components/HoldingsTable';
import PriceChart from '../components/PriceChart';
import TopHoldingsChart from '../components/TopHoldingsChart';

export default function UploadPage({ darkMode }) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [filename, setFilename] = useState(null);
  const [holdings, setHoldings] = useState([]);
  const [etfPrices, setEtfPrices] = useState([]);
  const [topHoldings, setTopHoldings] = useState([]);
  const [latestDate, setLatestDate] = useState(null);
  const [summary, setSummary] = useState(null);

  const handleUpload = useCallback(async (file) => {
    setIsLoading(true);
    setError(null);
    try {
      const formData = new FormData();
      formData.append('file', file);
      const response = await axios.post('/api/upload', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });
      const data = response.data;
      setFilename(data.filename);
      setSummary(data.summary);
      setHoldings(data.holdings);
      setEtfPrices(data.etf_prices);
      setTopHoldings(data.top_holdings);
      setLatestDate(data.latest_date);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to upload file.');
    } finally {
      setIsLoading(false);
    }
  }, []);

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
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Upload ETF</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Upload an ETF weights CSV to view in-memory analytics (v1 endpoint)
        </p>
      </div>

      {error && (
        <div className="p-4 bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 rounded-xl">
          <div className="flex items-center gap-2">
            <svg className="w-5 h-5 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <p className="text-sm text-red-700 dark:text-red-300">{error}</p>
          </div>
        </div>
      )}

      {!filename ? (
        <div className="max-w-xl">
          <FileUpload onUpload={handleUpload} isLoading={isLoading} />
          <div className="mt-6 p-4 bg-blue-50 dark:bg-blue-900/20 rounded-xl border border-blue-100 dark:border-blue-800/50">
            <h3 className="text-sm font-medium text-blue-800 dark:text-blue-300">How it works</h3>
            <ul className="mt-2 text-sm text-blue-700 dark:text-blue-400 list-disc list-inside space-y-1">
              <li>Upload an ETF weights CSV file (e.g., tech_leaders_etf.csv)</li>
              <li>View constituent holdings with their latest prices</li>
              <li>See the reconstructed ETF price over time</li>
              <li>Analyze top holdings by portfolio value</li>
            </ul>
          </div>
        </div>
      ) : (
        <>
          {/* File info bar */}
          <div className="flex items-center justify-between bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700/50 px-5 py-3">
            <div>
              <p className="text-sm font-medium text-gray-900 dark:text-white">{filename}</p>
              <p className="text-xs text-gray-500 dark:text-gray-400">
                {summary?.constituents} constituents • {(summary?.total_weight * 100).toFixed(1)}% total weight
              </p>
            </div>
            <button
              onClick={handleReset}
              className="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-200 bg-gray-100 dark:bg-gray-700 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
            >
              Upload New
            </button>
          </div>

          <PriceChart data={etfPrices} darkMode={darkMode} />

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <HoldingsTable holdings={holdings} latestDate={latestDate} />
            <TopHoldingsChart data={topHoldings} latestDate={latestDate} darkMode={darkMode} />
          </div>
        </>
      )}
    </div>
  );
}
