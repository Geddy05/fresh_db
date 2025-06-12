import React, { useEffect, useState } from 'react';

function App() {
  const [stats, setStats] = useState(null);

  useEffect(() => {
    const interval = setInterval(() => {
      fetch('http://127.0.0.1:8000/stats')
        .then((res) => res.json())
        .then(setStats)
        .catch(() => setStats(null));
    }, 2000);
    return () => clearInterval(interval);
  }, []);

  if (!stats) return <div>Loading stats...</div>;

  return (
    <div style={{ padding: 24, fontFamily: 'sans-serif', maxWidth: 600 }}>
      <h1>FreshDB Statistics</h1>
      <p><b>Uptime:</b> {stats.uptime && stats.uptime.toFixed(1)} seconds</p>
      <p><b>Total tables:</b> {stats.total_tables}</p>
      
      <h2>Tables</h2>
      {Object.values(stats.tables).length === 0 ? (
        <p>No tables found.</p>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', marginTop: 10 }}>
          <thead>
            <tr>
              <th style={{ borderBottom: '1px solid #ccc', textAlign: 'left' }}>Name</th>
              <th style={{ borderBottom: '1px solid #ccc', textAlign: 'right' }}>Columns</th>
              <th style={{ borderBottom: '1px solid #ccc', textAlign: 'right' }}>Rows</th>
              <th style={{ borderBottom: '1px solid #ccc', textAlign: 'right' }}>Disk Usage (bytes)</th>
              <th style={{ borderBottom: '1px solid #ccc', textAlign: 'left' }}>Indexes</th>
              <th style={{ borderBottom: '1px solid #ccc', textAlign: 'center' }}>Has PK</th>
            </tr>
          </thead>
          <tbody>
            {Object.values(stats.tables).map((table) => (
              <tr key={table.name}>
                <td style={{ padding: 6 }}>{table.name}</td>
                <td style={{ textAlign: 'right', padding: 6 }}>{table.columns}</td>
                <td style={{ textAlign: 'right', padding: 6 }}>{table.rows}</td>
                <td style={{ textAlign: 'right', padding: 6 }}>{table.disk_usage_bytes}</td>
                <td style={{ padding: 6 }}>{table.indexes && table.indexes.length > 0 ? table.indexes.join(', ') : '—'}</td>
                <td style={{ textAlign: 'center', padding: 6 }}>{table.has_pk ? '✔️' : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

export default App;
