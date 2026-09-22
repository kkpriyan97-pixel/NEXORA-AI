"""WORLD M1 LAB — multilingual web research + forward-only next-candle learning.

This subsystem is isolated from live signal delivery. Web material produces
research hypotheses; live market data produces forward labels. Neither stream
can directly change the live Brain until explicit validation gates are met.

Targets:
- 15 calendar days
- >= 10,000 unique trading-related domains
- 24 language packs, with Unicode script detection
- M1/1-minute candle methods, indicators, microstructure, timing, technology,
  failure modes and validation methods
- next-1-minute direction prediction using information available before the
  target candle exists
"""
from __future__ import annotations

import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
import html
import json
import logging
import math
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
import unicodedata
from collections import defaultdict
from html.parser import HTMLParser
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import httpx

log = logging.getLogger("candice.m1lab")

TARGET_DAYS = 15
TARGET_DOMAINS = 10_000
TARGET_DAILY = math.ceil(TARGET_DOMAINS / TARGET_DAYS)
RUN_SECONDS = max(45, int(os.getenv("M1_RESEARCH_RUN_SECONDS", "60")))
MAX_CONCURRENCY = max(4, min(24, int(os.getenv("M1_RESEARCH_MAX_CONCURRENCY", "12"))))
DISCOVERY_QUERIES_PER_RUN = max(12, min(48, int(os.getenv("M1_RESEARCH_QUERIES_PER_RUN", "30"))))
FETCHES_PER_RUN = max(24, min(160, int(os.getenv("M1_RESEARCH_FETCHES_PER_RUN", "72"))))
HTTP_TIMEOUT = max(4.0, float(os.getenv("M1_RESEARCH_HTTP_TIMEOUT", "8")))
MAX_PAGE_BYTES = max(200_000, int(os.getenv("M1_RESEARCH_MAX_PAGE_BYTES", "700000")))
USER_AGENT = os.getenv("M1_RESEARCH_USER_AGENT", "NEXORA-M1-ResearchBot/1.0")
DB_URL = os.getenv("DATABASE_URL", "").strip()
SQLITE_PATH = os.getenv("M1_RESEARCH_SQLITE_PATH", "m1_world_learning.sqlite3")
DEFAULT_START_UTC = "2026-09-21T20:52:37+00:00"

LANGUAGE_QUERIES = {
    "en": ["1 minute trading next candle prediction","M1 scalping candlestick price action market structure",
           "1 minute breakout pullback retest EMA RSI VWAP","1 minute false signal volatility session filter",
           "one minute trading indicators backtest walk forward","1 minute candlestick machine learning forecasting"],
    "es": ["trading 1 minuto predicción próxima vela","scalping M1 acción del precio velas estructura mercado",
           "ruptura pullback retesteo EMA RSI VWAP 1 minuto","señales falsas volatilidad sesión"],
    "pt": ["trading 1 minuto previsão próxima vela","scalping M1 ação do preço velas estrutura mercado",
           "rompimento pullback reteste EMA RSI VWAP","sinais falsos volatilidade sessão"],
    "fr": ["trading 1 minute prédiction prochaine bougie","scalping M1 chandeliers action prix structure marché",
           "cassure pullback retest EMA RSI VWAP","faux signaux volatilité session"],
    "de": ["1 Minute Trading nächste Kerze Prognose","M1 Scalping Kerzen Price Action Marktstruktur",
           "Ausbruch Pullback Retest EMA RSI VWAP","Fehlsignale Volatilität Session"],
    "it": ["trading 1 minuto previsione candela successiva","scalping M1 candele price action struttura mercato",
           "breakout pullback retest EMA RSI VWAP","falsi segnali volatilità sessione"],
    "nl": ["1 minuut trading volgende candle voorspelling","M1 scalping candlesticks price action marktstructuur",
           "breakout pullback retest EMA RSI VWAP","valse signalen volatiliteit sessie"],
    "ru": ["торговля 1 минута прогноз следующей свечи","скальпинг M1 свечи прайс экшен структура рынка",
           "пробой откат ретест EMA RSI VWAP","ложные сигналы волатильность сессия"],
    "uk": ["трейдинг 1 хвилина прогноз наступної свічки","скальпінг M1 свічки прайс екшен структура ринку",
           "пробій відкат ретест EMA RSI VWAP","хибні сигнали волатильність сесія"],
    "tr": ["1 dakika trading sonraki mum tahmini","M1 scalping mum fiyat hareketi piyasa yapısı",
           "kırılım geri çekilme retest EMA RSI VWAP","sahte sinyal volatilite seans"],
    "ar": ["تداول دقيقة واحدة توقع الشمعة التالية","سكالبينغ M1 شموع حركة السعر هيكل السوق",
           "اختراق تصحيح إعادة اختبار EMA RSI VWAP","إشارات كاذبة تقلب جلسة"],
    "fa": ["معامله یک دقیقه پیش بینی کندل بعدی","اسکالپ M1 کندل پرایس اکشن ساختار بازار",
           "بریک اوت پولبک ریتست EMA RSI VWAP","سیگنال کاذب نوسان سشن"],
    "hi": ["1 मिनट ट्रेडिंग अगली कैंडल भविष्यवाणी","M1 स्कैल्पिंग कैंडल प्राइस एक्शन मार्केट स्ट्रक्चर",
           "ब्रेकआउट पुलबैक रिटेस्ट EMA RSI VWAP","फॉल्स सिग्नल वोलैटिलिटी सेशन"],
    "bn": ["১ মিনিট ট্রেডিং পরের ক্যান্ডেল পূর্বাভাস","M1 স্ক্যাল্পিং ক্যান্ডেল প্রাইস অ্যাকশন মার্কেট স্ট্রাকচার",
           "ব্রেকআউট পুলব্যাক রিটেস্ট EMA RSI VWAP","ফলস সিগন্যাল ভোলাটিলিটি সেশন"],
    "ur": ["ایک منٹ ٹریڈنگ اگلی کینڈل پیش گوئی","M1 اسکیلپنگ کینڈل پرائس ایکشن مارکیٹ اسٹرکچر",
           "بریک آؤٹ پل بیک ری ٹیسٹ EMA RSI VWAP","غلط سگنل اتار چڑھاؤ سیشن"],
    "zh": ["1分钟交易下一根K线预测","M1交易K线价格行为市场结构",
           "突破回踩重测 EMA RSI VWAP 1分钟","虚假信号波动率交易时段"],
    "ja": ["1分足トレード次のローソク足予測","M1スキャルピングローソク足プライスアクション市場構造",
           "ブレイクアウト押し目リテスト EMA RSI VWAP","ダマシボラティリティセッション"],
    "ko": ["1분봉 트레이딩 다음 캔들 예측","M1 스캘핑 캔들 가격행동 시장구조",
           "돌파 되돌림 리테스트 EMA RSI VWAP","가짜 신호 변동성 세션"],
    "vi": ["giao dịch 1 phút dự đoán nến tiếp theo","scalping M1 nến hành động giá cấu trúc thị trường",
           "breakout pullback retest EMA RSI VWAP","tín hiệu giả biến động phiên"],
    "th": ["เทรด 1 นาที คาดการณ์แท่งถัดไป","สเกลป์ M1 แท่งเทียน price action โครงสร้างตลาด",
           "breakout pullback retest EMA RSI VWAP","สัญญาณหลอก ความผันผวน session"],
    "id": ["trading 1 menit prediksi candle berikutnya","scalping M1 candlestick price action struktur pasar",
           "breakout pullback retest EMA RSI VWAP","sinyal palsu volatilitas sesi"],
    "ms": ["dagangan 1 minit ramalan candle seterusnya","scalping M1 candlestick price action struktur pasaran",
           "breakout pullback retest EMA RSI VWAP","isyarat palsu volatiliti sesi"],
    "he": ["מסחר דקה אחת חיזוי הנר הבא","סקאלפינג M1 נרות price action מבנה שוק",
           "פריצה pullback retest EMA RSI VWAP","איתותים שגויים תנודתיות סשן"],
    "el": ["trading 1 λεπτού πρόβλεψη επόμενου κεριού","M1 scalping κεριά price action δομή αγοράς",
           "breakout pullback retest EMA RSI VWAP","ψευδή σήματα μεταβλητότητα συνεδρία"],
}

METHODS = {
    "trend_following":["trend following","ema alignment","moving average","adx","supertrend","trendfolge","тенденция","趋势交易"],
    "breakout":["breakout","range breakout","opening range","breakout retest","rupture","пробой","突破"],
    "pullback_retest":["pullback","retest","pull-back","reteste","откат","ретест","回踩","押し目"],
    "reversal_rejection":["reversal","rejection","pin bar","engulfing","hammer","shooting star","反转","разворот"],
    "vwap_momentum":["vwap","volume weighted average price","momentum","volume spike","成交量","モメンタム"],
    "rsi_divergence":["rsi divergence","divergence rsi","дивергенция rsi","背离"],
    "bollinger_mean_reversion":["bollinger","bollinger bands","band touch","mean reversion","布林带"],
    "market_structure":["market structure","higher high","lower low","hh hl","lh ll","support resistance","liquidity sweep","order block"],
    "microstructure":["order flow","footprint","market profile","volume profile","bid ask","delta","tape reading"],
    "volatility_regime":["atr","volatility","range expansion","range contraction","squeeze","volatility compression"],
    "session_timing":["london session","new york session","asian session","session open","opening range"],
    "momentum_indicators":["macd","stochastic","cci","mfi","obv","roc"],
    "trend_indicators":["ichimoku","heikin ashi","parabolic sar","keltner","donchian"],
    "advanced_models":["machine learning","deep learning","random forest","xgboost","lstm","transformer","reinforcement learning","kalman","hidden markov","fourier","wavelet"],
    "pattern_recognition":["triangle","flag","wedge","double top","double bottom","head and shoulders","harmonic"],
    "risk_and_filtering":["risk management","position sizing","stop loss","take profit","no trade","filter","drawdown"],
    "validation":["backtest","walk forward","walk-forward","out of sample","out-of-sample","monte carlo","bootstrap","expectancy","sample size"],
}
QUALITY = {"cmegroup.com":.92,"sec.gov":1.0,"investor.gov":1.0,"tradingview.com":.68,
           "oanda.com":.72,"ig.com":.72,"fidelity.com":.80,"schwab.com":.80,"babypips.com":.58}
MARKETING = re.compile(r"(90\\s*%|95\\s*%|99\\s*%|100\\s*%|guaranteed|guarantee|sure win|no loss|ganancia garantizada|"
                       r"lucro garantido|profit garanti|гарантированн|稳赚|稳赚不赔)", re.I)
SEARCH_HOSTS={"google.com","googleusercontent.com","duckduckgo.com","bing.com","search.yahoo.com"}

# Task-verified research seeds: hypotheses/evidence only; never direct live overrides.
RESEARCH_SEEDS=[
{"source":"ScienceDirect / QREF 81 (Rif & Utz, 2021)","url":"https://www.sciencedirect.com/science/article/pii/S1062976921000922","lang":"en","method_id":"reversal_rejection","finding":"Extreme negative one-minute returns in a Nasdaq-100 study showed a 31% reversal in the subsequent minute; reversal was stronger in the most liquid/largest firms.","caution":"Sample-specific academic result; revalidate on the target feed."},
{"source":"Journal of Multinational Financial Management (2021)","url":"https://www.sciencedirect.com/science/article/pii/S1042444X21000402","lang":"en","method_id":"microstructure","finding":"Real-time buyer/seller trade imbalance and passive-order imbalance were reported to predict one-minute-ahead excess returns in Borsa Istanbul data.","caution":"Requires order/trade-flow features; OHLC alone cannot reproduce the same signal."},
{"source":"ScienceDirect high-frequency conditional-probability study","url":"https://www.sciencedirect.com/science/article/pii/S0378437113007140","lang":"en","method_id":"microstructure","finding":"A studied high-frequency stock exhibited short-horizon directional dependence; same-direction movements showed predictive structure up to roughly one minute before weakening.","caution":"Asset-specific result; must be revalidated."},
{"source":"Oxford Journal of Financial Econometrics (2023)","url":"https://academic.oup.com/jfec/article-abstract/21/2/485/6400345","lang":"en","method_id":"advanced_models","finding":"Regularized linear and tree-based models showed short-horizon intraday predictability in a 5-minute equity study, with ensemble models strong after transaction costs in that sample.","caution":"5-minute equity study, not direct proof for M1 OTC signals."},
{"source":"MQL5 Market Microstructure / Order Flow (2026)","url":"https://www.mql5.com/en/articles/22939","lang":"en","method_id":"microstructure","finding":"At one-minute resolution, OHLCV does not directly reveal aggressor-side order flow; order-flow proxies need separate treatment.","caution":"Methodological guidance, not standalone performance evidence."},
{"source":"TradingView 1-minute 2-bar continuation script","url":"https://www.tradingview.com/script/0URsJopj-BarrettFVG-2-Bar-Continuation-Scalper/","lang":"en","method_id":"breakout","finding":"A one-minute continuation hypothesis can require two same-direction closed candles, minimum body/range, higher second-candle volume and agreeing trend/session filters.","caution":"Community script methodology; independently validate."},
{"source":"FXGlory M1 guide updated 2026-09-19","url":"https://fxglory.com/learn/forex-strategies/1-minute-forex-strategy/","lang":"en","method_id":"market_structure","finding":"M1 is presented as entry timing inside a broader plan using market condition, session, spread, volatility and higher-timeframe context.","caution":"Educational retail source."},
{"source":"MQL5 M1 Gold scalping article (2026)","url":"https://www.mql5.com/en/blogs/post/773047","lang":"en","method_id":"risk_and_filtering","finding":"The article emphasizes filtering, closed-bar confirmation and avoiding sideways/fakeout/repainting conditions in M1 scalping.","caution":"Author experience; hypothesis only."},
{"source":"Spanish M1 guide (2026)","url":"https://mejorbrokerbinario.com/es/estrategias/1-minuto/","lang":"es","method_id":"volatility_regime","finding":"The source emphasizes noise and execution sensitivity on one-minute charts and the need for strict filters.","caution":"Retail educational source."},
{"source":"GitHub Intra-Minute Execution Timing","url":"https://github.com/KeyangPan/Intra-Minute-Execution-Timing","lang":"en","method_id":"advanced_models","finding":"A walk-forward project used top-of-book features and logistic regression to optimize within-minute execution timing, separating decision timestamps from raw tick fills.","caution":"Execution-timing study, not a directional signal system."}
]

class Parser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title=[];self.text=[];self.links=[];self.anchor=None;self.anchor_text=[];self.skip=0
    def handle_starttag(self,tag,attrs):
        tag=tag.lower();a=dict(attrs)
        if tag in {"script","style","noscript","svg","canvas"}:self.skip+=1;return
        if tag=="a" and not self.skip and a.get("href"):self.anchor=a["href"];self.anchor_text=[]
    def handle_endtag(self,tag):
        tag=tag.lower()
        if tag in {"script","style","noscript","svg","canvas"}:self.skip=max(0,self.skip-1);return
        if tag=="a" and self.anchor:
            self.links.append((self.anchor," ".join(self.anchor_text)));self.anchor=None;self.anchor_text=[]
    def handle_data(self,data):
        if self.skip:return
        s=" ".join(str(data).split())
        if not s:return
        self.text.append(s)
        if self.anchor is not None:self.anchor_text.append(s)

def clean_url(raw,base=None):
    if not raw:return None
    if base:raw=urljoin(base,raw)
    raw=html.unescape(str(raw).strip())
    if raw.startswith("//"):raw="https:"+raw
    p=urlparse(raw)
    if p.scheme not in {"http","https"} or not p.netloc:return None
    host=(p.hostname or "").lower()
    if not host or host=="localhost" or host.endswith(".local"):return None
    q={k:v for k,v in parse_qs(p.query).items() if not k.lower().startswith(("utm_","fbclid","gclid"))}
    query="&".join(f"{k}={quote_plus(v[0])}" for k,v in sorted(q.items()))
    return urlunparse((p.scheme,host,p.path or "/", "",query,""))

def unwrap(href):
    p=urlparse(html.unescape(href or ""))
    q=parse_qs(p.query)
    for k in ("uddg","url","target","q"):
        v=q.get(k)
        if v and urlparse(v[0]).scheme in {"http","https"}:return v[0]
    return href

def domain(url):return (urlparse(url).hostname or "").lower().removeprefix("www.")

def quality(d):
    d=domain("https://"+d) if "://" not in d else domain(d)
    if d in QUALITY:return QUALITY[d]
    if d.endswith(".gov") or d.endswith(".edu") or d.endswith(".ac.uk") or d.endswith(".edu.au"):return 1.0
    if d.endswith(".org"):return .76
    return .45

def language_from_text(text):
    c=defaultdict(int)
    for ch in text[:10000]:
        n=unicodedata.name(ch,"")
        if "ARABIC" in n:c["ar"]+=1
        elif "CYRILLIC" in n:c["ru"]+=1
        elif "HEBREW" in n:c["he"]+=1
        elif "GREEK" in n:c["el"]+=1
        elif "DEVANAGARI" in n:c["hi"]+=1
        elif "BENGALI" in n:c["bn"]+=1
        elif "THAI" in n:c["th"]+=1
        elif "HANGUL" in n:c["ko"]+=1
        elif "CJK" in n:c["zh"]+=1
        elif "HIRAGANA" in n or "KATAKANA" in n:c["ja"]+=1
    return max(c,key=c.get) if c else "en"

def normalize(text):return re.sub(r"\s+"," "," ".join(text.split())).strip()

def simhash(text):
    words=re.findall(r"\w+",normalize(text).lower(),re.UNICODE);v=[0]*64
    for w in words[:3000]:
        h=int(hashlib.blake2b(w.encode("utf-8","ignore"),digest_size=8).hexdigest(),16)
        for i in range(64):v[i]+=1 if (h>>i)&1 else -1
    return f"{sum((1<<i) for i,x in enumerate(v) if x>=0):016x}"

def hamming(a,b):
    try:return (int(a,16)^int(b,16)).bit_count()
    except Exception:return 64

def evidence_extract(text,title,url,lang):
    low=text.lower();out=[]
    for mid,terms in METHODS.items():
        hits=[t for t in terms if t in low]
        if not hits:continue
        sentences=re.split(r"(?<=[.!?。！？])\s+",text)
        ev=[s.strip() for s in sentences if any(t in s.lower() for t in hits) and 20<=len(s)<=1200][:8]
        if not ev:continue
        mflag=bool(MARKETING.search(text))
        m1_bonus=.25 if any(x in low for x in ("1 minute","1-minute","m1","1min","1分","دقيقة واحدة","1 minuto")) else 0
        specificity=min(1.0,.12*len(hits)+m1_bonus)
        score=max(0,min(1,quality(url)*.65+specificity*.35-(.30 if mflag else 0)))
        out.append({"method_id":mid,"url":url,"domain":domain(url),"language":lang,"title":title[:240],
                    "hits":hits[:20],"evidence":ev,"source_quality":round(quality(url),3),
                    "evidence_score":round(score,3),"marketing_claim":mflag})
    return out

class DB:
    def __init__(self):
        self.sqlite=not bool(DB_URL)
        if self.sqlite:
            self.cx=sqlite3.connect(SQLITE_PATH,check_same_thread=False)
            self.cx.execute("PRAGMA journal_mode=WAL")
            self._init_sqlite()
        else:
            self.cx=None
            self._ensure_pg_sync()
    def _ensure_pg_sync(self):
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:
                q.execute("""CREATE TABLE IF NOT EXISTS nexora_m1_sources(
                  domain TEXT PRIMARY KEY, first_seen TIMESTAMPTZ DEFAULT NOW(), last_seen TIMESTAMPTZ DEFAULT NOW(),
                  language TEXT, homepage TEXT, pages_scanned INTEGER NOT NULL DEFAULT 0)""")
                q.execute("""CREATE TABLE IF NOT EXISTS nexora_m1_pages(
                  url TEXT PRIMARY KEY, domain TEXT NOT NULL, fetched_at TIMESTAMPTZ DEFAULT NOW(), title TEXT,
                  fingerprint TEXT, simhash TEXT, content_chars INTEGER DEFAULT 0, status INTEGER, language TEXT)""")
                q.execute("""CREATE TABLE IF NOT EXISTS nexora_m1_evidence(
                  id BIGSERIAL PRIMARY KEY, method_id TEXT, url TEXT, domain TEXT, language TEXT,
                  evidence_score DOUBLE PRECISION, evidence_json JSONB, marketing_claim BOOLEAN DEFAULT FALSE,
                  created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(method_id,url))""")
                q.execute("""CREATE TABLE IF NOT EXISTS nexora_m1_forecasts(
                  pair TEXT, prediction_ts BIGINT, target_ts BIGINT, direction TEXT, probability DOUBLE PRECISION,
                  features JSONB, result TEXT, created_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY(pair,prediction_ts))""")
                q.execute("""CREATE TABLE IF NOT EXISTS nexora_m1_weights(
                  name TEXT PRIMARY KEY, weight DOUBLE PRECISION NOT NULL)""")
                q.execute("""CREATE TABLE IF NOT EXISTS nexora_m1_meta(
                  key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
                q.execute("""CREATE TABLE IF NOT EXISTS nexora_m1_runs(
                  id BIGSERIAL PRIMARY KEY, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
                  discovered INTEGER, fetched INTEGER, evidence INTEGER, errors INTEGER)""")
            c.commit()
    def _init_sqlite(self):
        self.cx.executescript("""CREATE TABLE IF NOT EXISTS sources(domain TEXT PRIMARY KEY,language TEXT,homepage TEXT,first_seen REAL,last_seen REAL,pages_scanned INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS pages(url TEXT PRIMARY KEY,domain TEXT,title TEXT,fingerprint TEXT,simhash TEXT,fetched_at REAL,chars INTEGER,status INTEGER,language TEXT);
        CREATE TABLE IF NOT EXISTS evidence(id INTEGER PRIMARY KEY AUTOINCREMENT,method_id TEXT,url TEXT,domain TEXT,language TEXT,evidence_score REAL,evidence_json TEXT,marketing_claim INTEGER,created_at REAL,UNIQUE(method_id,url));
        CREATE TABLE IF NOT EXISTS forecasts(pair TEXT,prediction_ts INTEGER,target_ts INTEGER,direction TEXT,probability REAL,features TEXT,result TEXT,created_at REAL,PRIMARY KEY(pair,prediction_ts));
        CREATE TABLE IF NOT EXISTS weights(name TEXT PRIMARY KEY,weight REAL);CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS runs(id INTEGER PRIMARY KEY AUTOINCREMENT,started_at REAL,finished_at REAL,discovered INTEGER,fetched INTEGER,evidence INTEGER,errors INTEGER);""");self.cx.commit()
    def meta(self,k,d=""):
        if self.sqlite:
            r=self.cx.execute("SELECT value FROM meta WHERE key=?",(k,)).fetchone();return r[0] if r else d
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:q.execute("SELECT value FROM nexora_m1_meta WHERE key=%s",(k,));r=q.fetchone();return r[0] if r else d
    def set_meta(self,k,v):
        if self.sqlite:self.cx.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",(k,str(v)));self.cx.commit();return
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:q.execute("INSERT INTO nexora_m1_meta(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",(k,str(v)));c.commit()
    def seed_research_seeds(self):
        if self.sqlite:
            for e in RESEARCH_SEEDS:
                payload=json.dumps(e,ensure_ascii=False,separators=(",",":"))
                self.cx.execute("INSERT OR IGNORE INTO evidence(method_id,url,domain,language,evidence_score,evidence_json,marketing_claim,created_at) VALUES(?,?,?,?,?,?,?,?)",
                                (e["method_id"],e["url"],domain(e["url"]),e["lang"],round(min(1.0,quality(e["url"])),3),payload,0,time.time()))
                self.cx.execute("INSERT OR IGNORE INTO sources(domain,language,homepage,first_seen,last_seen) VALUES(?,?,?,?,?)",
                                (domain(e["url"]),e["lang"],e["url"],time.time(),time.time()))
            self.cx.commit()
            return
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as cdb:
            with cdb.cursor() as q:
                for e in RESEARCH_SEEDS:
                    d=domain(e["url"])
                    q.execute("INSERT INTO nexora_m1_sources(domain,language,homepage) VALUES(%s,%s,%s) ON CONFLICT(domain) DO NOTHING",(d,e["lang"],e["url"]))
                    q.execute("INSERT INTO nexora_m1_evidence(method_id,url,domain,language,evidence_score,evidence_json,marketing_claim) VALUES(%s,%s,%s,%s,%s,%s::jsonb,FALSE) ON CONFLICT(method_id,url) DO NOTHING",
                              (e["method_id"],e["url"],d,e["lang"],round(min(1.0,quality(e["url"])),3),json.dumps(e,ensure_ascii=False,separators=(",",":"))))
            cdb.commit()

    def count_pages(self):
        if self.sqlite:
            return int(self.cx.execute("SELECT COUNT(*) FROM pages").fetchone()[0])
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as cdb:
            with cdb.cursor() as q:
                q.execute("SELECT COUNT(*) FROM nexora_m1_pages")
                return int(q.fetchone()[0])

    def count_domains(self):
        if self.sqlite:return int(self.cx.execute("SELECT COUNT(*) FROM sources").fetchone()[0])
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:q.execute("SELECT COUNT(*) FROM nexora_m1_sources");return int(q.fetchone()[0])
    def page_seen(self,u):
        if self.sqlite:return self.cx.execute("SELECT 1 FROM pages WHERE url=?",(u,)).fetchone() is not None
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:q.execute("SELECT 1 FROM nexora_m1_pages WHERE url=%s",(u,));return q.fetchone() is not None
    def save_source(self,u,lang):
        d=domain(u);now=time.time()
        if self.sqlite:
            self.cx.execute("INSERT OR IGNORE INTO sources(domain,language,homepage,first_seen,last_seen) VALUES(?,?,?,?,?)",(d,lang,u,now,now));self.cx.execute("UPDATE sources SET last_seen=?,language=COALESCE(language,?) WHERE domain=?",(now,lang,d));self.cx.commit();return
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:q.execute("INSERT INTO nexora_m1_sources(domain,language,homepage) VALUES(%s,%s,%s) ON CONFLICT(domain) DO UPDATE SET last_seen=NOW(),language=COALESCE(nexora_m1_sources.language,EXCLUDED.language)",(d,lang,u));c.commit()
    def save_page_and_evidence(self,u,title,fp,sh,chars,status,lang,evidence):
        if self.sqlite:
            self.cx.execute("INSERT OR REPLACE INTO pages(url,domain,title,fingerprint,simhash,fetched_at,chars,status,language) VALUES(?,?,?,?,?,?,?,?,?)",(u,domain(u),title,fp,sh,time.time(),chars,status,lang))
            self.cx.execute("UPDATE sources SET pages_scanned=pages_scanned+1,last_seen=? WHERE domain=?",(time.time(),domain(u)))
            for e in evidence:self.cx.execute("INSERT OR IGNORE INTO evidence(method_id,url,domain,language,evidence_score,evidence_json,marketing_claim,created_at) VALUES(?,?,?,?,?,?,?,?)",(e["method_id"],e["url"],e["domain"],e["language"],e["evidence_score"],json.dumps(e,ensure_ascii=False),int(e["marketing_claim"]),time.time()))
            self.cx.commit();return
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:
                q.execute("INSERT INTO nexora_m1_pages(url,domain,title,fingerprint,simhash,content_chars,status,language) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(url) DO UPDATE SET fetched_at=NOW(),title=EXCLUDED.title,fingerprint=EXCLUDED.fingerprint,simhash=EXCLUDED.simhash,content_chars=EXCLUDED.content_chars,status=EXCLUDED.status,language=EXCLUDED.language",(u,domain(u),title,fp,sh,chars,status,lang))
                q.execute("UPDATE nexora_m1_sources SET pages_scanned=pages_scanned+1,last_seen=NOW() WHERE domain=%s",(domain(u),))
                for e in evidence:q.execute("INSERT INTO nexora_m1_evidence(method_id,url,domain,language,evidence_score,evidence_json,marketing_claim) VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s) ON CONFLICT(method_id,url) DO NOTHING",(e["method_id"],e["url"],e["domain"],e["language"],e["evidence_score"],json.dumps(e,ensure_ascii=False),bool(e["marketing_claim"])))
            c.commit()
    def store_forecast(self,pair,pred_ts,target_ts,direction,p,features):
        data=(pair,pred_ts,target_ts,direction,float(p),json.dumps(features,separators=(",",":")),time.time())
        if self.sqlite:
            self.cx.execute("INSERT OR IGNORE INTO forecasts(pair,prediction_ts,target_ts,direction,probability,features,created_at) VALUES(?,?,?,?,?,?,?)",data);self.cx.commit();return
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:q.execute("INSERT INTO nexora_m1_forecasts(pair,prediction_ts,target_ts,direction,probability,features) VALUES(%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT(pair,prediction_ts) DO NOTHING",(pair,pred_ts,target_ts,direction,float(p),json.dumps(features)));c.commit()
    def settle_forecast(self,pair,pred_ts,result):
        if self.sqlite:self.cx.execute("UPDATE forecasts SET result=? WHERE pair=? AND prediction_ts=?",(result,pair,pred_ts));self.cx.commit();return
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:q.execute("UPDATE nexora_m1_forecasts SET result=%s WHERE pair=%s AND prediction_ts=%s",(result,pair,pred_ts));c.commit()
    def forecast_stats(self):
        if self.sqlite:r=self.cx.execute("SELECT COUNT(*) n,SUM(result='WIN'),SUM(result='LOSS') FROM forecasts WHERE result IS NOT NULL").fetchone()
        else:
            import psycopg
            with psycopg.connect(DB_URL,connect_timeout=8) as c:
                with c.cursor() as q:q.execute("SELECT COUNT(*) n,COUNT(*) FILTER(WHERE result='WIN'),COUNT(*) FILTER(WHERE result='LOSS') FROM nexora_m1_forecasts WHERE result IS NOT NULL");r=q.fetchone()
        n=int(r[0] or 0);w=int(r[1] or 0);l=int(r[2] or 0)
        return {"samples":n,"wins":w,"losses":l,"accuracy":round(100*w/max(1,w+l),2)}

class NextCandleModel:
    FEATURES=("bias","ret1","ret3","ret5","body","range_norm","upper_wick","lower_wick","close_pos","ema_gap","rsi_gap","atr_norm","range_ratio","streak")
    def __init__(self,db):
        self.db=db;self.w={n:0.0 for n in self.FEATURES};self.w.update(self._load_weights())
    def _load_weights(self):
        try:
            if self.db.sqlite:r=self.db.cx.execute("SELECT name,weight FROM weights").fetchall();return {x[0]:float(x[1]) for x in r}
            import psycopg
            with psycopg.connect(DB_URL,connect_timeout=8) as c:
                with c.cursor() as q:q.execute("SELECT name,weight FROM nexora_m1_weights");return {x[0]:float(x[1]) for x in q.fetchall()}
        except Exception:return {}
    def save_weights(self):
        if self.db.sqlite:
            self.db.cx.executemany("INSERT OR REPLACE INTO weights(name,weight) VALUES(?,?)",list(self.w.items()));self.db.cx.commit();return
        import psycopg
        with psycopg.connect(DB_URL,connect_timeout=8) as c:
            with c.cursor() as q:q.executemany("INSERT INTO nexora_m1_weights(name,weight) VALUES(%s,%s) ON CONFLICT(name) DO UPDATE SET weight=EXCLUDED.weight",list(self.w.items()));c.commit()
    @staticmethod
    def ema(v,n):
        if not v:return 0.0
        k=2/(n+1);e=v[0]
        for x in v[1:]:e=x*k+e*(1-k)
        return e
    @staticmethod
    def rsi(v,n=14):
        if len(v)<n+1:return 50.0
        g=[];l=[]
        for a,b in zip(v[-n-1:-1],v[-n:]):
            d=b-a;g.append(max(0,d));l.append(max(0,-d))
        ag=sum(g)/n;al=sum(l)/n
        return 100 if al==0 else 100-(100/(1+ag/al))
    def features(self,cs):
        if len(cs)<25:return None
        c=cs[-1];cl=[float(x["close"]) for x in cs];px=max(abs(c["close"]),1e-9);rng=max(c["high"]-c["low"],1e-9)
        body=(c["close"]-c["open"])/rng;upper=(c["high"]-max(c["open"],c["close"]))/rng;lower=(min(c["open"],c["close"])-c["low"])/rng
        close_pos=((c["close"]-c["low"])/rng)*2-1
        ema9=self.ema(cl[-40:],9);ema21=self.ema(cl[-40:],21);rr=self.rsi(cl)
        tr=[] 
        for i in range(max(1,len(cs)-14),len(cs)):tr.append(max(cs[i]["high"]-cs[i]["low"],abs(cs[i]["high"]-cs[i-1]["close"]),abs(cs[i]["low"]-cs[i-1]["close"])))
        atr=sum(tr)/max(1,len(tr))
        rets=[(cl[-1]/cl[i]-1)*100 for i in (len(cl)-2,len(cl)-4,len(cl)-6) if cl[i]]
        while len(rets)<3:rets.append(0.0)
        ranges=[max(x["high"]-x["low"],1e-9) for x in cs[-20:]];med=sorted(ranges)[len(ranges)//2]
        streak=0
        for i in range(len(cl)-1,0,-1):
            if (cl[i]>cl[i-1])==(cl[-1]>cl[-2]):streak+=1
            else:break
        return {"bias":1.0,"ret1":rets[0],"ret3":rets[1],"ret5":rets[2],"body":body,"range_norm":rng/px*100,
                "upper_wick":upper,"lower_wick":lower,"close_pos":close_pos,"ema_gap":(ema9-ema21)/px*100,
                "rsi_gap":(rr-50)/50,"atr_norm":atr/px*100,"range_ratio":rng/max(med,1e-9),"streak":min(streak,8)/8}
    def predict(self,f):
        z=max(-8,min(8,sum(self.w.get(k,0)*v for k,v in f.items())));p=1/(1+math.exp(-z));return ("UP" if p>=.5 else "DOWN"),p
    def update(self,f,label,lr=.025):
        p=self.predict(f)[1];y=1 if label=="UP" else 0;err=y-p
        for k,v in f.items():self.w[k]=max(-8,min(8,self.w.get(k,0)+lr*err*v))
        self.save_weights()

class M1WorldLab:
    def __init__(self):
        self.db=DB();self.model=NextCandleModel(self.db);self.queue=asyncio.Queue()
        self.enqueued=set();self.robots={};self.simhash_cache=[];self.pending={}
        self.started_at=float(self.db.meta("started_at", "0") or 0)
        if not self.started_at:
            raw=os.getenv("M1_LEARNING_START_UTC",DEFAULT_START_UTC)
            try:self.started_at=datetime.fromisoformat(raw.replace("Z","+00:00")).astimezone(timezone.utc).timestamp()
            except Exception:self.started_at=time.time()
            self.db.set_meta("started_at",self.started_at)
        self.db.seed_research_seeds()
        self.metrics={"runs":0,"searches":0,"discovered":0,"fetched":0,"evidence":0,"errors":0,"deduped":0}
        self.languages_seen=defaultdict(int)
    def day(self):
        return min(TARGET_DAYS,max(1,int(max(0,time.time()-self.started_at)//86400)+1))
    def status(self):
        domains=self.db.count_domains();remain=max(0,TARGET_DOMAINS-domains)
        return {"enabled":os.getenv("M1_WORLD_LEARNING_ENABLED","true").lower()!="false",
                "day":self.day(),"days":TARGET_DAYS,"stage":self.stage(),"target_domains":TARGET_DOMAINS,
                "unique_domains":domains,"remaining_domains":remain,"progress_pct":round(domains/TARGET_DOMAINS*100,2),
                "daily_target":TARGET_DAILY,"model":self.db.forecast_stats(),"metrics":dict(self.metrics),
                "languages_seen":dict(sorted(self.languages_seen.items(),key=lambda x:-x[1])[:24])}
    def stage(self):
        stages=["M1 foundations + structure","candle geometry + next-candle behaviour","EMA/RSI/MACD/Stochastic",
                "VWAP/ATR/volatility","breakout + retest","pullback + continuation","reversal + rejection",
                "support/resistance + liquidity","session/time-of-day","false signals + no-trade filters",
                "microstructure + volume","indicator combinations","ML/statistical forecasting",
                "walk-forward/anti-overfit","own-strategy consolidation + audit"]
        return stages[self.day()-1]
    def enqueue(self,u,lang,origin):
        u=clean_url(u)
        if not u or self.db.page_seen(u) or u in self.enqueued:return
        self.enqueued.add(u)
        try:self.queue.put_nowait((u,lang,origin));self.metrics["discovered"]+=1
        except asyncio.QueueFull:pass
    async def robots_ok(self,u):
        d=domain(u);now=time.time()
        c=self.robots.get(d)
        if c and now-c[0]<21600:return True if c[1] is None else c[1].can_fetch(USER_AGENT,u)
        rp=RobotFileParser()
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(5,connect=3),headers={"User-Agent":USER_AGENT},follow_redirects=True) as h:
                r=await h.get("https://"+d+"/robots.txt");rp=None if r.status_code>=400 else rp
                if r.status_code<400:rp.parse(r.text.splitlines())
        except Exception:rp=None
        self.robots[d]=(now,rp);return True if rp is None else rp.can_fetch(USER_AGENT,u)
    async def fetch(self,u):
        async with httpx.AsyncClient(timeout=httpx.Timeout(HTTP_TIMEOUT,connect=3),headers={"User-Agent":USER_AGENT,"Accept":"text/html,application/xhtml+xml"},follow_redirects=True) as h:
            r=await h.get(u);r.raise_for_status()
            if "html" not in r.headers.get("content-type","").lower():return "","",r.status_code,[]
            p=Parser();p.feed(r.content[:MAX_PAGE_BYTES].decode(r.encoding or "utf-8","ignore"))
            return normalize(" ".join(p.text)),normalize(" ".join(p.title)),r.status_code,p.links
    async def crawl(self,item):
        u,lang,origin=item
        if not await self.robots_ok(u):return
        try:
            body,title,status,links=await self.fetch(u)
            if len(body)<160:return
            det=language_from_text(body) if lang in {"auto","unknown"} else lang
            sh=simhash(body)
            if self.simhash_cache and any(hamming(sh,x)<=5 for x in self.simhash_cache[-2500:]):
                self.metrics["deduped"]+=1
            else:
                self.simhash_cache.append(sh)
                self.db.save_source(u,det)
                ev=evidence_extract(body,title,u,det)
                self.db.save_page_and_evidence(u,title,hashlib.sha256(body[:20000].encode("utf-8","ignore")).hexdigest(),sh,len(body),status,det,ev)
                self.metrics["fetched"]+=1;self.languages_seen[det]+=1;self.metrics["evidence"]+=len(ev)
            d=domain(u);added=0
            for href,label in links:
                if added>=6:break
                v=clean_url(unwrap(href),u)
                if not v or domain(v)!=d:continue
                x=(v+" "+label).lower()
                if any(bad in x for bad in ("/login","/signup","/register","/checkout")):continue
                if any(k in x for k in ("m1","1-minute","1minute","next-candle","prediction","candlestick","scalp","strategy",
                                        "breakout","pullback","retest","ema","rsi","vwap","bollinger","atr","order-flow","volume-profile","backtest","walk-forward")):
                    self.enqueue(v,det,"internal")
                    added+=1
        except Exception:
            self.metrics["errors"]+=1
    async def search(self,q,lang):
        self.metrics["searches"]+=1
        endpoints=("https://html.duckduckgo.com/html/?q="+quote_plus(q),
                   "https://www.google.com/search?q="+quote_plus(q)+"&num=20",
                   "https://www.bing.com/search?q="+quote_plus(q))
        for ep in endpoints:
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(8,connect=3),headers={"User-Agent":USER_AGENT},follow_redirects=True) as h:
                    r=await h.get(ep)
                if r.status_code>=400:continue
                p=Parser()
                p.feed(r.text)
                found=0
                for href,label in p.links:
                    v=clean_url(unwrap(href))
                    if not v or domain(v) in SEARCH_HOSTS:continue
                    self.enqueue(v,lang,"search")
                    found+=1
                if found:return found
            except Exception as exc:
                self.metrics["errors"]+=1
                log.debug("M1_SEARCH_ERROR query=%s error=%s",q,exc)
        return 0

    async def discover(self):
        items=[]
        langs=list(LANGUAGE_QUERIES)
        offset=(self.metrics["runs"]*7+self.day()*3)%len(langs)
        for i in range(DISCOVERY_QUERIES_PER_RUN):
            lang=langs[(offset+i)%len(langs)];qs=LANGUAGE_QUERIES[lang]
            q=qs[(self.metrics["runs"]+i+self.day())%len(qs)]
            # Add one method/technology term to widen semantic coverage.
            method=list(METHODS)[(self.metrics["runs"]+i)%len(METHODS)]
            items.append((f"{q} {method.replace('_',' ')}",lang))
        return await asyncio.gather(*(self.search(q,l) for q,l in items),return_exceptions=True)
    def snapshot(self,pair,candles,timestamp=None):
        cs=[]
        for c in candles or []:
            if not isinstance(c,dict):continue
            try:
                t=float(c.get("time",c.get("t")));o=float(c.get("open",c.get("o")));h=float(c.get("high",c.get("h")))
                lo=float(c.get("low",c.get("l")));cl=float(c.get("close",c.get("c")))
                if t>20_000_000_000:t/=1000
                if t+60>float(timestamp or time.time())-1:continue
                cs.append({"time":int(t//60)*60,"open":o,"high":h,"low":lo,"close":cl})
            except Exception:continue
        cs=list({x["time"]:x for x in cs}.values());cs.sort(key=lambda x:x["time"])
        if len(cs)<26:return
        ts=cs[-1]["time"]
        pending=self.pending.get(pair)
        if pending and int(pending["target_ts"])==int(ts):
            actual="UP" if cs[-1]["close"]>pending["anchor_close"] else "DOWN" if cs[-1]["close"]<pending["anchor_close"] else "TIE"
            result="TIE" if actual=="TIE" else "WIN" if actual==pending["direction"] else "LOSS"
            self.db.settle_forecast(pair,pending["prediction_ts"],result)
            if actual in {"UP","DOWN"}:self.model.update(pending["features"],actual)
            self.pending.pop(pair,None)
        f=self.model.features(cs)
        if not f:return
        direction,p=self.model.predict(f);pred_ts=int(ts);target_ts=pred_ts+60
        self.db.store_forecast(pair,pred_ts,target_ts,direction,p,f)
        self.pending[pair]={"prediction_ts":pred_ts,"target_ts":target_ts,"direction":direction,"probability":p,
                            "features":f,"anchor_close":float(cs[-1]["close"])}
    def synthesize(self):
        fs=self.db.forecast_stats()
        status="WATCH"
        if fs["samples"]>=100:status="FORWARD_VALIDATED_CANDIDATE" if fs["accuracy"]>=55 else "RESEARCH_REJECT"
        domains=self.db.count_domains()
        return {"program":{"days":TARGET_DAYS,"day":self.day(),"target_domains":TARGET_DOMAINS,"unique_domains":domains,
                           "stage":self.stage()},"next_candle_model":{"status":status,**fs},
                "rules":{"forward_only":True,"no_lookahead":True,"web_claims_not_profitability_proof":True,
                         "live_brain_activation":False},"status":self.status()}
    async def run_once(self):
        self.metrics["runs"]+=1;start=time.time()
        await self.discover()
        workers=[]
        while not self.queue.empty() and len(workers)<min(MAX_CONCURRENCY*3,FETCHES_PER_RUN):
            workers.append(asyncio.create_task(self.crawl(await self.queue.get())))
        if workers:await asyncio.gather(*workers,return_exceptions=True)
        status=self.synthesize()
        global _STATUS_CACHE
        _STATUS_CACHE=status.get("status") or {}
        self.db.set_meta("last_status",json.dumps(_STATUS_CACHE,separators=(",",":")))
        log.info("M1_WORLD_RESEARCH day=%d domains=%d/%d pages=%d evidence=%d model=%s languages=%s",
                 self.day(),self.db.count_domains(),TARGET_DOMAINS,self.db.count_pages(),self.metrics["evidence"],
                 self.db.forecast_stats(),dict(self.languages_seen))
        return status
    async def run_forever(self):
        if os.getenv("M1_WORLD_LEARNING_ENABLED","true").strip().lower()=="false":
            log.info("M1_WORLD_LEARNING_DISABLED");return
        while True:
            try:await self.run_once()
            except asyncio.CancelledError:raise
            except Exception as e:log.exception("M1_WORLD_LEARNING_RUN_FAILED %s",e)
            await asyncio.sleep(RUN_SECONDS)

LAB=M1WorldLab()

# The research lab performs synchronous database writes and CPU work. Keep both
# research and live-snapshot learning off the trading event loop so a slow
# database/search cannot delay the exact signal boundary.
RESEARCH_EXECUTOR=ThreadPoolExecutor(max_workers=1,thread_name_prefix="m1research")
SNAPSHOT_EXECUTOR=ThreadPoolExecutor(max_workers=1,thread_name_prefix="m1snapshot")
_STATUS_CACHE=LAB.status()

def learning_status():
    return dict(_STATUS_CACHE)

def record_market_snapshot(pair,candles,timestamp=None):
    LAB.snapshot(pair,candles,timestamp)

async def record_market_snapshot_async(pair,candles,timestamp=None):
    loop=asyncio.get_running_loop()
    return await loop.run_in_executor(
        SNAPSHOT_EXECUTOR,
        record_market_snapshot,
        pair,candles,timestamp
    )

async def world_learning_loop():
    loop=asyncio.get_running_loop()
    def runner():
        asyncio.run(LAB.run_forever())
    await loop.run_in_executor(RESEARCH_EXECUTOR,runner)
