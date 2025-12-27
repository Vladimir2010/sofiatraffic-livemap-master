import express from 'express';
import fetch from 'node-fetch';

const app = express();

app.get('/api/virtual-board', async (req, res) => {
  try {
    const stop = req.query.stop_code;
    if (!stop) {
      return res.status(400).json({ error: 'stop_code is required' });
    }

    const r = await fetch(
      `https://sofiatraffic-proxy.onrender.com/v2/virtual-board?stop_code=${stop}`,
      {
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'Mozilla/5.0',
          'Origin': 'http://127.0.0.1:1234'
        }
      }
    );

    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: 'proxy failed' });
  }
});

app.listen(3000, () => console.log('Backend on http://localhost:3000'));
