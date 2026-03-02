import { useState, useEffect } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import OverviewPage from './pages/OverviewPage';
import ETFExplorerPage from './pages/ETFExplorerPage';
import AnalyticsPage from './pages/AnalyticsPage';
import PipelinePage from './pages/PipelinePage';
import UploadPage from './pages/UploadPage';

export default function App() {
  const [darkMode, setDarkMode] = useState(() => {
    if (typeof window !== 'undefined') {
      return localStorage.getItem('darkMode') === 'true' ||
        window.matchMedia('(prefers-color-scheme: dark)').matches;
    }
    return false;
  });

  const [collapsed, setCollapsed] = useState(false);

  useEffect(() => {
    if (darkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    localStorage.setItem('darkMode', darkMode);
  }, [darkMode]);

  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-50 dark:bg-gray-950 transition-colors duration-200">
        <Sidebar
          darkMode={darkMode}
          setDarkMode={setDarkMode}
          collapsed={collapsed}
          setCollapsed={setCollapsed}
        />

        {/* Main content area — offset by sidebar width */}
        <main className={`transition-all duration-300 ${collapsed ? 'ml-16' : 'ml-56'}`}>
          <div className="max-w-7xl mx-auto px-6 py-8">
            <Routes>
              <Route path="/" element={<OverviewPage darkMode={darkMode} />} />
              <Route path="/etfs" element={<ETFExplorerPage darkMode={darkMode} />} />
              <Route path="/analytics" element={<AnalyticsPage darkMode={darkMode} />} />
              <Route path="/pipeline" element={<PipelinePage darkMode={darkMode} />} />
              <Route path="/upload" element={<UploadPage darkMode={darkMode} />} />
            </Routes>
          </div>
        </main>
      </div>
    </BrowserRouter>
  );
}
