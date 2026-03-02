/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: 'class',
  safelist: [
    // Dynamic classes used in PipelinePage architecture diagram
    'bg-blue-50', 'bg-amber-50', 'bg-purple-50', 'bg-green-50', 'bg-indigo-50',
    'bg-blue-500', 'bg-amber-500', 'bg-purple-500', 'bg-green-500', 'bg-indigo-500',
    'text-blue-700', 'text-amber-700', 'text-purple-700', 'text-green-700', 'text-indigo-700',
    'dark:bg-blue-900/20', 'dark:bg-amber-900/20', 'dark:bg-purple-900/20', 'dark:bg-green-900/20', 'dark:bg-indigo-900/20',
    'dark:text-blue-300', 'dark:text-amber-300', 'dark:text-purple-300', 'dark:text-green-300', 'dark:text-indigo-300',
  ],
  theme: {
    extend: {
      colors: {
        gray: {
          950: '#030712',
        },
      },
    },
  },
  plugins: [],
}
