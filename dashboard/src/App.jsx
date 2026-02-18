import { useState, useEffect, useCallback } from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, ReferenceLine,
} from 'recharts';
import AdminPage from './AdminPage';
import {
  API_BASE, INTENSITY_COLORS, FUEL_COLORS, fetchJSON as fetchFromAPI,
  LiveDot, StatusBadge, Skeleton, Card, CardTitle, timeAgo,
} from './shared';
import './index.css';

// ─── Configuration ───
const REFRESH_MS = 5 * 60 * 1000; // 5 minutes
const USE_MOCK = true; // Falls back to mock if API unavailable

// ─── Mock Data Generator ───
function generateMockData() {
  const now = new Date();
  const hour = now.getHours();

  // Realistic diurnal pattern for NYISO
  const diurnalBase = (h) => {
    const rad = ((h - 3) / 24) * Math.PI * 2;
    return 240 + 110 * Math.sin(rad) + (Math.random() - 0.5) * 30;
  };

  const currentCI = diurnalBase(hour);
  const category = currentCI <= 150 ? 'very_clean' : currentCI <= 250 ? 'clean' :
                    currentCI <= 350 ? 'moderate' : currentCI <= 450 ? 'dirty' : 'very_dirty';

  // Fuel mix (realistic NYISO proportions)
  const totalMW = 18000 + Math.random() * 6000;
  const gasPct = 0.3 + 0.15 * Math.sin(((hour - 3) / 24) * Math.PI * 2);
  const fuelMix = {
    'Natural Gas':        Math.round(totalMW * gasPct),
    'Dual Fuel':          Math.round(totalMW * (gasPct * 0.6)),
    'Nuclear':            Math.round(totalMW * 0.18),
    'Hydro':              Math.round(totalMW * 0.15),
    'Wind':               Math.round(totalMW * (0.06 + Math.random() * 0.06)),
    'Other Renewables':   Math.round(totalMW * 0.02),
    'Other Fossil Fuels': Math.round(totalMW * 0.01),
  };

  const cleanMW = fuelMix['Nuclear'] + fuelMix['Hydro'] + fuelMix['Wind'] + fuelMix['Other Renewables'];
  const actualTotal = Object.values(fuelMix).reduce((a, b) => a + b, 0);

  // 24h forecast
  const forecast = Array.from({ length: 24 }, (_, i) => {
    const h = (hour + i) % 24;
    const ci = diurnalBase(h);
    const cat = ci <= 150 ? 'very_clean' : ci <= 250 ? 'clean' :
                ci <= 350 ? 'moderate' : ci <= 450 ? 'dirty' : 'very_dirty';
    const forecastTime = new Date(now.getTime() + i * 3600000);
    return {
      hour: forecastTime.toISOString(),
      time_label: forecastTime.toLocaleTimeString('en-US', { hour: 'numeric', hour12: true }),
      grams_co2_per_kwh: Math.round(ci),
      category: cat,
      label: INTENSITY_COLORS[cat] ? cat.replace('_', ' ') : cat,
      confidence: i < 6 ? 'high' : i < 18 ? 'medium' : 'low',
    };
  });

  // Find cleanest/dirtiest 3h windows
  let bestAvg = Infinity, bestIdx = 0, worstAvg = -Infinity, worstIdx = 0;
  for (let i = 0; i <= forecast.length - 3; i++) {
    const avg = (forecast[i].grams_co2_per_kwh + forecast[i+1].grams_co2_per_kwh + forecast[i+2].grams_co2_per_kwh) / 3;
    if (avg < bestAvg) { bestAvg = avg; bestIdx = i; }
    if (avg > worstAvg) { worstAvg = avg; worstIdx = i; }
  }

  return {
    current: {
      timestamp: now.toISOString(),
      carbon_intensity: {
        grams_co2_per_kwh: Math.round(currentCI),
        category,
        label: category.replace('_', ' '),
      },
      recommendation: category === 'very_clean' ? 'Great time to run energy-intensive tasks!' :
                      category === 'clean' ? 'Good time for discretionary electricity use.' :
                      category === 'moderate' ? 'Grid is average. Defer if you can wait.' :
                      category === 'dirty' ? 'Consider waiting — grid is carbon-heavy.' :
                      'Worst time for electricity. Defer everything you can.',
      generation: {
        total_mw: actualTotal,
        clean_percentage: Math.round((cleanMW / actualTotal) * 100 * 10) / 10,
        fuel_breakdown_mw: fuelMix,
        fuel_percentages: Object.fromEntries(
          Object.entries(fuelMix).map(([k, v]) => [k, Math.round(v / actualTotal * 1000) / 10])
        ),
      },
    },
    forecast: {
      hourly: forecast,
      cleanest_3h_window: {
        start: forecast[bestIdx].hour,
        end: forecast[Math.min(bestIdx + 3, forecast.length - 1)].hour,
        avg_grams_co2_per_kwh: Math.round(bestAvg),
        start_label: forecast[bestIdx].time_label,
        end_label: forecast[Math.min(bestIdx + 2, forecast.length - 1)].time_label,
      },
      dirtiest_3h_window: {
        start: forecast[worstIdx].hour,
        end: forecast[Math.min(worstIdx + 3, forecast.length - 1)].hour,
        avg_grams_co2_per_kwh: Math.round(worstAvg),
        start_label: forecast[worstIdx].time_label,
        end_label: forecast[Math.min(worstIdx + 2, forecast.length - 1)].time_label,
      },
    },
  };
}

// ─── Chart Components ───
function ForecastChart({ data }) {
  if (!data || data.length === 0) return <Skeleton className="h-64 w-full" />;

  const chartData = data.map((d, i) => ({
    ...d,
    index: i,
    displayTime: d.time_label || new Date(d.hour).toLocaleTimeString('en-US', { hour: 'numeric', hour12: true }),
    fill: INTENSITY_COLORS[d.category]?.hex || '#eab308',
  }));

  const CustomTooltip = ({ active, payload }) => {
    if (!active || !payload?.[0]) return null;
    const d = payload[0].payload;
    const c = INTENSITY_COLORS[d.category] || INTENSITY_COLORS.moderate;
    return (
      <div className="bg-carbon-850 border border-carbon-700/60 rounded-xl p-3 shadow-2xl backdrop-blur-md">
        <p className="text-xs text-gray-400 font-display mb-1">{d.displayTime} · {d.confidence} confidence</p>
        <p className="text-lg font-bold font-data" style={{ color: c.text }}>
          {d.grams_co2_per_kwh} <span className="text-xs font-normal text-gray-500">gCO₂/kWh</span>
        </p>
      </div>
    );
  };

  return (
    <ResponsiveContainer width="100%" height="100%">
      <AreaChart data={chartData} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
        <defs>
          <linearGradient id="forecastGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#eab308" stopOpacity={0.35} />
            <stop offset="40%" stopColor="#22c55e" stopOpacity={0.15} />
            <stop offset="100%" stopColor="#22c55e" stopOpacity={0.02} />
          </linearGradient>
          <linearGradient id="lineGradient" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="#4ade80" />
            <stop offset="50%" stopColor="#facc15" />
            <stop offset="100%" stopColor="#f97316" />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
        <XAxis
          dataKey="displayTime"
          tick={{ fontSize: 10, fill: '#6B7280', fontFamily: 'DM Mono' }}
          axisLine={{ stroke: 'rgba(255,255,255,0.06)' }}
          tickLine={false}
          interval={Math.max(1, Math.floor(chartData.length / 8))}
        />
        <YAxis
          tick={{ fontSize: 10, fill: '#6B7280', fontFamily: 'DM Mono' }}
          axisLine={false}
          tickLine={false}
          domain={['auto', 'auto']}
          tickFormatter={(v) => `${v}`}
        />
        <Tooltip content={<CustomTooltip />} cursor={{ stroke: 'rgba(255,255,255,0.1)', strokeWidth: 1 }} />
        <Area
          type="monotone"
          dataKey="grams_co2_per_kwh"
          stroke="url(#lineGradient)"
          strokeWidth={2.5}
          fill="url(#forecastGradient)"
          dot={false}
          activeDot={{ r: 5, fill: '#FFD60A', stroke: '#0A0A0F', strokeWidth: 2 }}
        />
        {/* Now marker */}
        {chartData.length > 0 && (
          <ReferenceLine
            x={chartData[0].displayTime}
            stroke="rgba(255,214,10,0.4)"
            strokeDasharray="4 4"
            label={{ value: 'NOW', position: 'top', fill: '#FFD60A', fontSize: 9, fontFamily: 'DM Mono' }}
          />
        )}
      </AreaChart>
    </ResponsiveContainer>
  );
}

function FuelMixChart({ fuelData }) {
  if (!fuelData) return <Skeleton className="h-48 w-full" />;

  const data = Object.entries(fuelData)
    .map(([name, mw]) => ({ name, value: mw, color: FUEL_COLORS[name] || '#6B7280' }))
    .filter(d => d.value > 0)
    .sort((a, b) => b.value - a.value);

  const total = data.reduce((s, d) => s + d.value, 0);

  return (
    <div className="flex items-center gap-4">
      <div className="w-36 h-36 flex-shrink-0">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={data}
              cx="50%" cy="50%"
              innerRadius={38} outerRadius={58}
              paddingAngle={2}
              dataKey="value"
              stroke="none"
            >
              {data.map((d, i) => <Cell key={i} fill={d.color} />)}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
      </div>
      <div className="flex-1 space-y-1.5 min-w-0">
        {data.map((d) => (
          <div key={d.name} className="flex items-center gap-2 text-xs">
            <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: d.color }} />
            <span className="text-gray-400 truncate flex-1 font-display">{d.name}</span>
            <span className="font-data text-gray-300 flex-shrink-0">
              {Math.round(d.value / total * 100)}%
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Main Dashboard ───
function Dashboard() {
  const [data, setData] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [isLive, setIsLive] = useState(false);
  const [error, setError] = useState(null);
  const [tick, setTick] = useState(0); // For relative time updates

  const fetchData = useCallback(async () => {
    // Try live API first
    const [currentData, forecastData] = await Promise.all([
      fetchFromAPI('/now'),
      fetchFromAPI('/forecast?hours=24'),
    ]);

    if (currentData && forecastData) {
      // Process forecast data to add time labels
      if (forecastData.hourly) {
        forecastData.hourly = forecastData.hourly.map(h => ({
          ...h,
          time_label: new Date(h.hour).toLocaleTimeString('en-US', { hour: 'numeric', hour12: true }),
        }));
      }
      setData({ current: currentData, forecast: forecastData });
      setIsLive(true);
      setError(null);
    } else if (USE_MOCK) {
      setData(generateMockData());
      setIsLive(false);
      setError(null);
    } else {
      setError('Could not connect to gridcarbon API');
    }
    setLastUpdated(new Date().toISOString());
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, REFRESH_MS);
    return () => clearInterval(interval);
  }, [fetchData]);

  // Tick for "last updated" relative time
  useEffect(() => {
    const t = setInterval(() => setTick(p => p + 1), 30000);
    return () => clearInterval(t);
  }, []);

  if (error && !data) {
    return (
      <div className="min-h-screen flex items-center justify-center p-8">
        <Card className="max-w-md text-center">
          <p className="text-red-400 font-display text-lg mb-2">Connection Error</p>
          <p className="text-gray-400 text-sm">
            Could not reach the gridcarbon API at {API_BASE}.
            Make sure the server is running: <code className="font-data text-canary-500">gridcarbon serve</code>
          </p>
        </Card>
      </div>
    );
  }

  const ci = data?.current?.carbon_intensity;
  const cat = ci?.category || 'moderate';
  const colors = INTENSITY_COLORS[cat];
  const gen = data?.current?.generation;
  const fc = data?.forecast;
  const cleanest = fc?.cleanest_3h_window;
  const dirtiest = fc?.dirtiest_3h_window;

  // Trend: compare current to forecast average
  const forecastAvg = fc?.hourly
    ? Math.round(fc.hourly.reduce((s, h) => s + h.grams_co2_per_kwh, 0) / fc.hourly.length)
    : null;
  const trend = ci && forecastAvg
    ? ci.grams_co2_per_kwh > forecastAvg ? 'improving' : ci.grams_co2_per_kwh < forecastAvg ? 'worsening' : 'stable'
    : null;

  return (
    <div className="min-h-screen">
      {/* ─── Header ─── */}
      <header className="border-b border-carbon-700/30 bg-carbon-950/80 backdrop-blur-md sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <svg viewBox="0 0 24 24" className="w-5 h-5 text-canary-500" fill="currentColor">
                <path d="M12 2C10 2 8.5 3.5 8 5.5C7 5 5.5 5 4.5 6C3 7.5 3 10 4 12C3 13 2 15 3 17C4 19 6 20 8 20C8 21 9 22 11 22C13 22 14 21.5 15 20.5C17 20.5 19 19 20 17C21 15 20.5 13 19.5 11.5C21 9.5 21 7 19.5 5.5C18 4 16 4 14.5 4.5C13.5 3 12.5 2 12 2Z"/>
              </svg>
              <span className="font-display font-semibold text-lg tracking-tight">
                <span className="text-canary-500">Canary</span>
              </span>
            </div>
            <span className="text-[10px] font-data text-carbon-600 border border-carbon-700/50 rounded px-1.5 py-0.5 uppercase">
              NYISO
            </span>
          </div>
          <div className="flex items-center gap-4 text-xs text-gray-500">
            <Link to="/admin" className="font-display text-gray-400 hover:text-canary-500 transition-colors">
              Admin
            </Link>
            <div className="flex items-center gap-2">
              <LiveDot color={isLive ? '#22c55e' : '#FFD60A'} />
              <span className="font-display">
                {isLive ? 'Live' : 'Demo'} · {timeAgo(lastUpdated)}
              </span>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-5">

        {/* ─── Row 1: Hero + Forecast ─── */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-5">

          {/* Hero Metric */}
          <Card className="lg:col-span-4" glowColor={colors?.hex}>
            <CardTitle icon="⚡">Current Intensity</CardTitle>
            {ci ? (
              <div>
                <div className="flex items-baseline gap-2 mb-1">
                  <span className="text-5xl font-bold font-data tracking-tight" style={{ color: colors?.text }}>
                    {ci.grams_co2_per_kwh}
                  </span>
                  <span className="text-sm text-gray-500 font-display">gCO₂/kWh</span>
                </div>
                <div className="flex items-center gap-2 mb-4">
                  <StatusBadge category={cat} />
                  {trend && (
                    <span className={`text-xs font-display ${
                      trend === 'improving' ? 'text-green-400' :
                      trend === 'worsening' ? 'text-orange-400' : 'text-gray-500'
                    }`}>
                      {trend === 'improving' ? '↓ trending cleaner' :
                       trend === 'worsening' ? '↑ expected to rise' : '→ stable'}
                    </span>
                  )}
                </div>
                <p className="text-sm text-gray-400 font-display leading-relaxed mb-4">
                  {data.current.recommendation}
                </p>
                {gen && (
                  <div className="space-y-2 pt-3 border-t border-carbon-700/30">
                    <div className="flex justify-between text-xs">
                      <span className="text-gray-500 font-display">Total Generation</span>
                      <span className="font-data text-gray-300">{Math.round(gen.total_mw).toLocaleString()} MW</span>
                    </div>
                    <div className="flex justify-between text-xs">
                      <span className="text-gray-500 font-display">Clean Energy</span>
                      <span className="font-data text-green-400">{gen.clean_percentage}%</span>
                    </div>
                    {/* Clean energy bar */}
                    <div className="h-1.5 bg-carbon-700/50 rounded-full overflow-hidden">
                      <div
                        className="h-full rounded-full transition-all duration-1000 ease-out"
                        style={{
                          width: `${gen.clean_percentage}%`,
                          background: 'linear-gradient(90deg, #22c55e, #3b82f6)',
                        }}
                      />
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="space-y-3">
                <Skeleton className="h-14 w-40" />
                <Skeleton className="h-6 w-24" />
                <Skeleton className="h-12 w-full" />
              </div>
            )}
          </Card>

          {/* Forecast Chart */}
          <Card className="lg:col-span-8">
            <div className="flex items-center justify-between mb-2">
              <CardTitle icon="📈">24-Hour Forecast</CardTitle>
              {forecastAvg && (
                <span className="text-xs font-data text-gray-500">
                  avg {forecastAvg} gCO₂/kWh
                </span>
              )}
            </div>
            <div className="h-56 md:h-64">
              <ForecastChart data={fc?.hourly} />
            </div>
          </Card>
        </div>

        {/* ─── Row 2: Fuel Mix + Windows + Recommendation ─── */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-12 gap-5">

          {/* Fuel Mix */}
          <Card className="lg:col-span-4">
            <CardTitle icon="🔋">Fuel Mix</CardTitle>
            <FuelMixChart fuelData={gen?.fuel_breakdown_mw} />
          </Card>

          {/* Best / Worst Windows */}
          <Card className="lg:col-span-4">
            <CardTitle icon="🕐">Optimal Windows</CardTitle>
            {cleanest && dirtiest ? (
              <div className="space-y-4">
                {/* Cleanest */}
                <div className="rounded-xl p-3.5" style={{ backgroundColor: 'rgba(34,197,94,0.08)', border: '1px solid rgba(34,197,94,0.15)' }}>
                  <div className="flex items-center gap-2 mb-1.5">
                    <span className="text-green-400 text-sm">✦</span>
                    <span className="text-xs font-medium text-green-400 uppercase tracking-wider font-display">Best Time</span>
                  </div>
                  <p className="text-lg font-bold font-data text-green-300">
                    {cleanest.start_label || new Date(cleanest.start).toLocaleTimeString('en-US', { hour: 'numeric', hour12: true })}
                    {' – '}
                    {cleanest.end_label || new Date(cleanest.end).toLocaleTimeString('en-US', { hour: 'numeric', hour12: true })}
                  </p>
                  <p className="text-xs text-gray-400 font-data mt-1">
                    ~{cleanest.avg_grams_co2_per_kwh} gCO₂/kWh avg
                  </p>
                </div>

                {/* Dirtiest */}
                <div className="rounded-xl p-3.5" style={{ backgroundColor: 'rgba(239,68,68,0.06)', border: '1px solid rgba(239,68,68,0.12)' }}>
                  <div className="flex items-center gap-2 mb-1.5">
                    <span className="text-red-400 text-sm">⚠</span>
                    <span className="text-xs font-medium text-red-400 uppercase tracking-wider font-display">Avoid</span>
                  </div>
                  <p className="text-lg font-bold font-data text-red-300">
                    {dirtiest.start_label || new Date(dirtiest.start).toLocaleTimeString('en-US', { hour: 'numeric', hour12: true })}
                    {' – '}
                    {dirtiest.end_label || new Date(dirtiest.end).toLocaleTimeString('en-US', { hour: 'numeric', hour12: true })}
                  </p>
                  <p className="text-xs text-gray-400 font-data mt-1">
                    ~{dirtiest.avg_grams_co2_per_kwh} gCO₂/kWh avg
                  </p>
                </div>
              </div>
            ) : (
              <div className="space-y-3">
                <Skeleton className="h-24 w-full" />
                <Skeleton className="h-24 w-full" />
              </div>
            )}
          </Card>

          {/* Grid Status / Info */}
          <Card className="lg:col-span-4">
            <CardTitle icon="ℹ️">About This Data</CardTitle>
            <div className="space-y-3 text-xs text-gray-400 font-display leading-relaxed">
              <p>
                <span className="text-gray-300 font-medium">Average carbon intensity</span> calculated
                from NYISO's 5-minute real-time fuel mix data using EPA eGRID emission factors.
              </p>
              <p>
                <span className="text-gray-300 font-medium">Forecast</span> uses historical
                patterns (hour-of-day × month) with temperature and wind speed corrections.
                No ML — heuristic model with ~15% MAPE.
              </p>
              <div className="pt-2 border-t border-carbon-700/30 space-y-1.5">
                <div className="flex justify-between">
                  <span className="text-gray-500">Region</span>
                  <span className="font-data text-gray-300">NYISO (NY State)</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Resolution</span>
                  <span className="font-data text-gray-300">5-min (fuel mix)</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Methodology</span>
                  <span className="font-data text-gray-300">Direct combustion</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Factors source</span>
                  <span className="font-data text-gray-300">EPA eGRID 2022</span>
                </div>
              </div>
            </div>
          </Card>
        </div>

        {/* ─── Row 3: Forecast Detail Table ─── */}
        <Card>
          <CardTitle icon="📊">Hourly Detail</CardTitle>
          <div className="overflow-x-auto -mx-6 px-6">
            <div className="flex gap-1 min-w-max pb-2">
              {fc?.hourly?.map((h, i) => {
                const c = INTENSITY_COLORS[h.category] || INTENSITY_COLORS.moderate;
                const barHeight = Math.max(8, Math.min(80, (h.grams_co2_per_kwh - 100) / 4));
                return (
                  <div key={i} className="flex flex-col items-center gap-1 group cursor-default" style={{ minWidth: '2.2rem' }}>
                    <span className="text-[9px] font-data text-gray-600 opacity-0 group-hover:opacity-100 transition-opacity">
                      {h.grams_co2_per_kwh}
                    </span>
                    <div className="w-full flex items-end justify-center" style={{ height: '80px' }}>
                      <div
                        className="w-4 rounded-t transition-all duration-300 group-hover:w-5"
                        style={{
                          height: `${barHeight}px`,
                          backgroundColor: c.hex,
                          opacity: i === 0 ? 1 : 0.65 + (h.confidence === 'high' ? 0.35 : h.confidence === 'medium' ? 0.15 : 0),
                        }}
                      />
                    </div>
                    <span className={`text-[9px] font-data ${i === 0 ? 'text-canary-500 font-medium' : 'text-gray-600'}`}>
                      {h.time_label || new Date(h.hour).toLocaleTimeString('en-US', { hour: 'numeric', hour12: true })}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </Card>

      </main>

      {/* Footer */}
      <footer className="border-t border-carbon-700/20 mt-8">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex items-center justify-between text-xs text-gray-600 font-display">
          <span>Canary · gridcarbon v0.1.0</span>
          <span>
            Data: NYISO · EPA eGRID · Open-Meteo
          </span>
        </div>
      </footer>
    </div>
  );
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<Dashboard />} />
      <Route path="/admin" element={<AdminPage />} />
    </Routes>
  );
}

export default App;
