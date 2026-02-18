// ─── Shared components, utilities, and constants ───
// Extracted to avoid circular imports between App.jsx and AdminPage.jsx.

// ─── Configuration ───
import { API_BASE } from './config';
export { API_BASE };

// ─── Color System ───
export const INTENSITY_COLORS = {
  very_clean: { bg: 'rgba(34,197,94,0.12)', text: '#4ade80', ring: 'rgba(34,197,94,0.25)', hex: '#22c55e' },
  clean:      { bg: 'rgba(132,204,22,0.12)', text: '#a3e635', ring: 'rgba(132,204,22,0.25)', hex: '#84cc16' },
  moderate:   { bg: 'rgba(250,204,21,0.12)', text: '#facc15', ring: 'rgba(250,204,21,0.25)', hex: '#eab308' },
  dirty:      { bg: 'rgba(249,115,22,0.12)', text: '#fb923c', ring: 'rgba(249,115,22,0.25)', hex: '#f97316' },
  very_dirty: { bg: 'rgba(239,68,68,0.12)',  text: '#f87171', ring: 'rgba(239,68,68,0.25)',  hex: '#ef4444' },
};

export const FUEL_COLORS = {
  'Natural Gas':       '#F59E0B',
  'Dual Fuel':         '#D97706',
  'Nuclear':           '#A78BFA',
  'Hydro':             '#3B82F6',
  'Wind':              '#06B6D4',
  'Other Renewables':  '#10B981',
  'Other Fossil Fuels':'#6B7280',
};

// ─── Data Fetching ───
export async function fetchJSON(endpoint) {
  try {
    const resp = await fetch(`${API_BASE}${endpoint}`, { signal: AbortSignal.timeout(5000) });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json();
  } catch {
    return null;
  }
}

// ─── Utility Components ───
export function LiveDot({ color = '#22c55e' }) {
  return (
    <span className="relative flex h-2.5 w-2.5">
      <span className="animate-ping absolute inline-flex h-full w-full rounded-full opacity-60"
            style={{ backgroundColor: color }} />
      <span className="relative inline-flex rounded-full h-2.5 w-2.5"
            style={{ backgroundColor: color }} />
    </span>
  );
}

export function StatusBadge({ category }) {
  const c = INTENSITY_COLORS[category] || INTENSITY_COLORS.moderate;
  const labels = {
    very_clean: 'Very Clean', clean: 'Clean', moderate: 'Moderate',
    dirty: 'Dirty', very_dirty: 'Very Dirty',
  };
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium font-display"
          style={{ backgroundColor: c.bg, color: c.text, boxShadow: `inset 0 0 0 1px ${c.ring}` }}>
      <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: c.text }} />
      {labels[category] || category}
    </span>
  );
}

export function Skeleton({ className = '' }) {
  return <div className={`animate-pulse bg-carbon-700/50 rounded-lg ${className}`} />;
}

export function Card({ children, className = '', glowColor = null }) {
  return (
    <div className={`relative bg-carbon-900/80 backdrop-blur-sm rounded-2xl border border-carbon-700/30 p-6 card-glow transition-all duration-300 ${className}`}>
      {glowColor && (
        <div className="absolute inset-0 rounded-2xl glow-shift pointer-events-none"
             style={{ background: `radial-gradient(ellipse 80% 60% at 20% 20%, ${glowColor}, transparent 70%)`, opacity: 0.15 }} />
      )}
      <div className="relative z-10">{children}</div>
    </div>
  );
}

export function CardTitle({ children, icon = null }) {
  return (
    <div className="flex items-center gap-2 mb-4">
      {icon && <span className="text-gray-500">{icon}</span>}
      <h3 className="text-xs font-medium text-gray-500 uppercase tracking-widest font-display">{children}</h3>
    </div>
  );
}

export function timeAgo(isoString) {
  if (!isoString) return '';
  const diff = Date.now() - new Date(isoString).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  return `${hrs}h ago`;
}
