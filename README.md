README.md

Nuristani Signal — Replit Starter (Node.js backend + React frontend)

این پروژه یک شروع سریع (starter) برای اپلیکیشن "Nuristani Signal" است که روی Replit قابل اجراست. کدها طوری طراحی شده‌اند که به صورت زنده از Binance websocket kline streams کندل‌ها را دریافت کنند، شاخص‌های تکنیکال محاسبه کنند، و سیگنال‌های کوتاه‌مدت (intervalهای 1m, 3m, 5m, 15m, 30m و یک 10m مشتق‌شده) را به کلاینت (واسط کاربری) ارسال کنند.

فایل‌های موجود در این پروژه:

package.json (ریشه)

server.js (بک‌اند اصلی — WebSocket به Binance، محاسبه اندیکاتورها، و socket.io برای ارسال سیگنال‌ها)

client/ (ری‌اکت ساده برای نمایش سیگنال‌ها)

client/package.json

client/src/App.jsx

client/src/index.js

client/public/index.html




---

راه‌اندازی در Replit (مراحل سریع)

1. در Replit یک Repl جدید بسازید: نوع Node.js.


2. محتویات این پروژه را (فایل‌ها را) در Replit قرار دهید.


3. در ریشه ترمینال اجرا کنید:

npm install
cd client
npm install
npm run build
cd ..
npm start

یا فقط npm start اگر اسکریپت‌های build را از قبل اجرا کرده باشید.


4. برای اینکه اپ همیشه آنلاین بماند، در Replit از ویژگی "Always On" یا power-up استفاده کنید.




---

متغیرهای محیطی (ENV)

SYMBOLS — لیستی از نمادها که می‌خواهید دنبال شوند، جدا شده با کاما. مثال: BTCUSDT,ETHUSDT (پیشفرض: BTCUSDT)

PORT — پورت سرور (پیشفرض 3000)



---

نکات طراحی و کاری که انجام می‌دهد

از Binance WebSocket kline streams برای دریافت کندل‌های زمان‌بندی‌شده استفاده می‌شود (هر کندل وقتی بسته می‌شود، محاسبه انجام می‌گیرد).

برای intervalهای استاندارد مثل 1m/3m/5m/15m/30m مستقیماً از کی‌لاین‌های بایننس استفاده می‌کنیم. (برای 10m که بایننس به طور پیشفرض ندارد، از 1m کندل‌ها 10تایی تجمیع می‌کنیم.)

از کتابخانه technicalindicators جهت محاسبه EMA,R SI, MACD, ATR و غیره استفاده شده است.

الگوریتم پیش‌بینی اولیه بر پایه‌ی یک مدل امتیازدهی قاعده‌محور (rule-based scoring) است که خروجی به‌صورت:

جهتِ تخمینی (up/down/neutral)

درصد حرکت مورد انتظار (estimated %)

احتمال / confidence (0..1) ارائه می‌دهد.




---

FILE: package.json

{
  "name": "nuristani-signal",
  "version": "1.0.0",
  "main": "server.js",
  "scripts": {
    "start": "node server.js",
    "start:dev": "nodemon server.js",
    "client": "cd client && npm start",
    "build": "cd client && npm run build"
  },
  "dependencies": {
    "express": "^4.18.2",
    "ws": "^8.13.0",
    "socket.io": "^4.7.1",
    "technicalindicators": "^1.0.20",
    "axios": "^1.4.0",
    "node-cron": "^3.0.0",
    "cors": "^2.8.5"
  }
}


---

FILE: server.js

// Nuristani Signal - simple Node backend
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const TI = require('technicalindicators');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const PORT = process.env.PORT || 3000;
const SYMBOLS = (process.env.SYMBOLS || 'BTCUSDT').split(',').map(s => s.trim().toUpperCase());
const INTERVALS = ['1m','3m','5m','15m','30m']; // 10m will be derived from 1m

// in-memory store (برای شروع)
const candles = {}; // candles[symbol][interval] = [ {t,o,h,l,c,v} ... ]
const latestSignals = {}; // latestSignals[symbol_interval] = signal

// helper
function ensure(symbol, interval){
  candles[symbol] = candles[symbol] || {};
  candles[symbol][interval] = candles[symbol][interval] || [];
}

// connect to Binance websocket for a symbol/interval
function connectKline(symbol, interval){
  ensure(symbol, interval);
  const url = `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@kline_${interval}`;
  const ws = new WebSocket(url);
  ws.on('open', ()=> console.log('WS open', symbol, interval));
  ws.on('message', msg => {
    try{
      const data = JSON.parse(msg);
      const k = data.k;
      if(k && k.x){ // candle closed
        const candle = {
          t: k.t,
          o: parseFloat(k.o),
          h: parseFloat(k.h),
          l: parseFloat(k.l),
          c: parseFloat(k.c),
          v: parseFloat(k.v)
        };
        candles[symbol][interval].push(candle);
        // keep memory bounded
        if(candles[symbol][interval].length > 1000) candles[symbol][interval].shift();
        // compute
        computeSignalFor(symbol, interval);
        // also recompute derived 10m if we have 1m data
        if(interval === '1m') computeDerived10m(symbol);
      }
    }catch(e){ console.error('ws msg err', e); }
  });
  ws.on('close', ()=>{ console.log('WS closed', symbol, interval); setTimeout(()=>connectKline(symbol, interval), 5000); });
  ws.on('error', err => { console.error('WS error', err); ws.terminate(); });
}

function computeDerived10m(symbol){
  ensure(symbol, '1m');
  const arr1 = candles[symbol]['1m'];
  if(arr1.length < 10) return;
  const last10 = arr1.slice(-10);
  const open = last10[0].o;
  const high = Math.max(...last10.map(x=>x.h));
  const low = Math.min(...last10.map(x=>x.l));
  const close = last10[last10.length-1].c;
  const vol = last10.reduce((s,x)=>s+x.v,0);
  ensure(symbol, '10m');
  candles[symbol]['10m'] = candles[symbol]['10m'] || [];
  candles[symbol]['10m'].push({t: last10[0].t, o:open, h:high, l:low, c:close, v:vol});
  if(candles[symbol]['10m'].length > 1000) candles[symbol]['10m'].shift();
  computeSignalFor(symbol, '10m');
}

function computeSignalFor(symbol, interval){
  const data = (candles[symbol] && candles[symbol][interval]) || [];
  if(data.length < 30) return; // صبر می‌کنیم تا داده کافی
  const closes = data.map(d=>d.c);
  const highs = data.map(d=>d.h);
  const lows = data.map(d=>d.l);

  // indicators
  const ema12 = TI.EMA.calculate({period:12, values:closes});
  const ema26 = TI.EMA.calculate({period:26, values:closes});
  const rsiArr = TI.RSI.calculate({period:14, values:closes});
  const macdArr = TI.MACD.calculate({values:closes, fastPeriod:12, slowPeriod:26, signalPeriod:9, SimpleMAOscillator:false, SimpleMASignal:false});
  const atrArr = TI.ATR.calculate({high:highs, low:lows, close:closes, period:14});
  if(!ema12.length || !ema26.length || !rsiArr.length || !macdArr.length || !atrArr.length) return;

  const lastPrice = closes[closes.length-1];
  const ema12Last = ema12[ema12.length-1];
  const ema26Last = ema26[ema26.length-1];
  const rsiLast = rsiArr[rsiArr.length-1];
  const macdLast = macdArr[macdArr.length-1];
  const atrLast = atrArr[atrArr.length-1];

  // simple rule-based scoring
  let score = 0;
  if(ema12Last > ema26Last) score += 1; else score -= 1;
  if(macdLast && macdLast.histogram > 0) score += 1; else score -= 1;
  if(rsiLast < 35) score += 1; else if(rsiLast > 65) score -= 1;

  // normalize
  const prob = 1/(1+Math.exp(-score));
  const volNorm = atrLast / lastPrice; // rough volatility
  const expectedMovePct = volNorm * score * 100 * 1.2; // تخمینی درصد حرکت
  const direction = expectedMovePct > 0 ? 'up' : (expectedMovePct < 0 ? 'down' : 'neutral');

  const signal = {
    symbol,
    interval,
    price: lastPrice,
    direction,
    expectedMovePct: Number(expectedMovePct.toFixed(3)),
    probability: Number(prob.toFixed(3)),
    timestamp: Date.now()
  };
  latestSignals[`${symbol}_${interval}`] = signal;
  io.emit('signal', signal);
}

app.get('/signals', (req, res) => {
  res.json(Object.values(latestSignals));
});

// start websocket connections
SYMBOLS.forEach(sym => {
  INTERVALS.forEach(iv => connectKline(sym, iv));
  // connect 1m separately and 10m is derived
  connectKline(sym, '1m');
});

server.listen(PORT, ()=> console.log('Server listening on', PORT));


---

FILE: client/package.json

{
  "name": "nuristani-signal-client",
  "version": "1.0.0",
  "private": true,
  "dependencies": {
    "react": "18.2.0",
    "react-dom": "18.2.0",
    "socket.io-client": "^4.7.1"
  },
  "scripts": {
    "start": "react-scripts start",
    "build": "react-scripts build",
    "test": "react-scripts test"
  }
}


---

FILE: client/src/App.jsx

import React, { useEffect, useState } from 'react';
import { io } from 'socket.io-client';
const socket = io();

export default function App(){
  const [signals, setSignals] = useState({});

  useEffect(()=>{
    socket.on('signal', s => {
      setSignals(prev => ({ ...prev, [s.symbol + '_' + s.interval]: s }));
    });
    return ()=> socket.off('signal');
  },[]);

  const rows = Object.values(signals).sort((a,b)=>b.timestamp - a.timestamp);
  return (
    <div style={{padding:20,fontFamily:'Arial'}}>
      <h1>Nuristani Signal - Live</h1>
      <table style={{width:'100%',borderCollapse:'collapse'}}>
        <thead>
          <tr><th>Symbol</th><th>Interval</th><th>Price</th><th>Direction</th><th>Exp. Move %</th><th>Confidence</th></tr>
        </thead>
        <tbody>
          {rows.map(s => (
            <tr key={s.symbol+s.interval} style={{borderTop:'1px solid #ddd'}}>
              <td>{s.symbol}</td>
              <td>{s.interval}</td>
              <td>{s.price}</td>
              <td>{s.direction}</td>
              <td>{s.expectedMovePct}%</td>
              <td>{s.probability}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}


---

FILE: client/src/index.js

import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
createRoot(document.getElementById('root')).render(<App />);


---

FILE: client/public/index.html

<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Nuristani Signal</title>
  </head>
  <body>
    <div id="root"></div>
    <script src="/socket.io/socket.io.js"></script>
  </body>
</html>




