/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      fontFamily: {
        display: ['Outfit', 'sans-serif'],
        mono: ['DM Mono', 'monospace'],
      },
      colors: {
        canary: { 400: '#FFE066', 500: '#FFD60A', 600: '#E6BF00' },
        carbon: {
          950: '#0A0A0F',
          900: '#111118',
          850: '#16161F',
          800: '#1C1C28',
          700: '#2A2A3A',
          600: '#3A3A4E',
        },
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
      },
    },
  },
  plugins: [],
}
