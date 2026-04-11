import { useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import OpportunityFeed from './components/OpportunityFeed';
import GraphExplorer   from './components/GraphExplorer';
import RagChat         from './components/RagChat';
import Dashboard       from './components/Dashboard';
import PipelineControl from './components/PipelineControl';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, staleTime: 30_000 },
  },
});

type Page = 'feed' | 'graph' | 'chat' | 'dashboard' | 'pipeline';

const NAV_ITEMS: { id: Page; label: string; icon: string }[] = [
  { id: 'feed',      label: 'Opportunities', icon: '⚡' },
  { id: 'graph',     label: 'Graph',         icon: '🕸' },
  { id: 'chat',      label: 'RAG Chat',      icon: '💬' },
  { id: 'dashboard', label: 'Dashboard',     icon: '📊' },
  { id: 'pipeline',  label: 'Pipeline',      icon: '⚙' },
];

function Inner() {
  const [page, setPage] = useState<Page>('feed');

  return (
    <div className="flex h-screen overflow-hidden bg-[#0f1117]">
      <nav className="w-14 flex-shrink-0 border-r border-[#2a2d3a] flex flex-col items-center py-4 gap-1">
        <div className="w-8 h-8 rounded-full bg-[#f59e0b] flex items-center justify-center text-black font-bold text-sm mb-4">
          E
        </div>
        {NAV_ITEMS.map(item => (
          <button
            key={item.id}
            onClick={() => setPage(item.id)}
            title={item.label}
            className={`w-10 h-10 rounded-xl flex items-center justify-center text-lg transition-colors ${
              page === item.id
                ? 'bg-[#14b8a6]/20 text-[#14b8a6]'
                : 'text-[#64748b] hover:text-[#e2e8f0] hover:bg-[#1a1d27]'
            }`}
          >
            {item.icon}
          </button>
        ))}
      </nav>
      <main className="flex-1 overflow-hidden">
        {page === 'feed'      && <OpportunityFeed />}
        {page === 'graph'     && <GraphExplorer />}
        {page === 'chat'      && <RagChat />}
        {page === 'dashboard' && <Dashboard />}
        {page === 'pipeline'  && <PipelineControl />}
      </main>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Inner />
    </QueryClientProvider>
  );
}
