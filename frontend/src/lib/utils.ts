import { clsx, type ClassValue } from 'clsx';

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs);
}

export function fmtScore(score: number): string {
  return `${Math.round(score * 100)}%`;
}

export function scoreColor(score: number): string {
  if (score >= 0.7) return '#22c55e';
  if (score >= 0.4) return '#eab308';
  return '#ef4444';
}

export function warmthLabel(score: number): string {
  if (score >= 1.0) return 'Hot';
  if (score >= 0.5) return 'Warm';
  if (score >= 0.1) return 'Cool';
  return 'Cold';
}

export function warmthColor(score: number): string {
  if (score >= 1.0) return '#f87171';
  if (score >= 0.5) return '#fbbf24';
  if (score >= 0.1) return '#60a5fa';
  return '#64748b';
}

export function nodeTypeColor(nodeType: string): string {
  const colors: Record<string, string> = {
    Person:    '#14b8a6',
    Company:   '#f87171',
    Publisher: '#a78bfa',
    Community: '#fbbf24',
    Ego:       '#f59e0b',
  };
  return colors[nodeType] || '#64748b';
}

export function priorityColor(priority: string): string {
  if (priority === 'High')   return '#22c55e';
  if (priority === 'Medium') return '#eab308';
  return '#64748b';
}

export function truncate(text: string, maxLen: number): string {
  if (!text || text.length <= maxLen) return text || '';
  return text.slice(0, maxLen) + '…';
}

export function timeSince(isoDate?: string): string {
  if (!isoDate) return 'never';
  const d = new Date(isoDate);
  const diff = Date.now() - d.getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export const VENTURE_LABELS: Record<string, string> = {
  applied_insights: 'Applied Insights',
  aegis_t2a: 'AEGIS-T2A',
  rgn_trucking: 'RGN Trucking',
  job_search: 'Job Search',
};

export const INTENT_LABELS: Record<string, string> = {
  Exploit: 'Exploit',
  Explore: 'Explore',
  Bridge:  'Bridge',
  Recruit: 'Recruit',
  Sell:    'Sell',
};
