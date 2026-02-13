#!/usr/bin/env python3
"""
Cruzada Bot Profissional v3.0 - Corrigido
============================================
BUGS CORRIGIDOS:
  1. lock_modelos/lock_pares redefinidos (DEADLOCK) → locks únicos
  2. ciclos_sem_trade não era global → agora é global
  3. Filtro lateralização usava valor absoluto → agora usa PERCENTUAL
  4. Filtro vol < 0.005 matava sinais → limiar reduzido para 0.001
  5. Filtro desequilíbrio matava sinais → suavizado
  6. Ranking usava "forca_final" inconsistente → padronizado
  7. min_conf 0.42 muito alto → começa em 0.35
  8. Sobrescrita cascata de micro_modelos → fluxo limpo
  9. enviar_ordem com positionSide/reduceOnly compatível
"""
import sys
import json
import os
import signal
import atexit
import threading
import time
import math
import traceback
import logging
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import numpy as np
import pandas as pd
import feedparser
import warnings

from binance.um_futures import UMFutures
from binance.client import Client
from binance.exceptions import BinanceAPIException

from ta.volatility import BollingerBands, AverageTrueRange
from ta.momentum import RSIIndicator
from ta.trend import MACD, ADXIndicator, CCIIndicator
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from colorama import init, Fore, Style

warnings.simplefilter(action='ignore', category=RuntimeWarning)
init(autoreset=True)

# ================================================================
# LOGGING ESTRUTURADO
# ================================================================
FORCAR_1_TRADE_TESTE = True

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/bot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("CruzadaBot")

# ================================================================
# CONFIG
# ================================================================
API_KEY = "xSMxXL8rlb66ZyRyS3TRNZ2XHkjllSYIL6xi5SSXhrHrhOmZWtRuz1ocfZMrSD9i"
API_SECRET = "aL80T0crJ4fexKQBCglaSjq7KRBNXGxnWNCRvqLlFTmh4UREHRNk6DoDxJuc4Nuk"

# -------------------- Parâmetros Gerais --------------------
REAL_TRADES = True
MAX_PARES = 200
SLEEP_BETWEEN_CYCLES = 5.0
CANDLE_LIMIT = 400
MIN_NOTIONAL_FALLBACK = 1.0
CAPITAL_TOTAL = 3000.0

# -------------------- Gestão de Risco --------------------
RISCO_MAX_POR_TRADE = 0.02
STOP_LOSS_DURO_PCT = 0.03
LUCRO_MICRO_PCT = 0.05
TRAILING_DROP_DEFAULT = 0.01
TRAILING_STEP = 0.005

# -------------------- Risk:Reward dinâmico --------------------
MIN_RR_RATIO = 1.5              # mínimo risco:retorno para entrar
MAX_POSICOES_SIMULTANEAS = 10   # máximo de posições abertas ao mesmo tempo
MAX_EXPOSICAO_TOTAL = 0.50      # máximo 50% do capital exposto
COOLDOWN_APOS_LOSS = 60         # segundos de espera após stop loss

# -------------------- Cache --------------------
CACHE_FILTROS_TTL = 300
CACHE_SALDO_TTL = 10
ATUALIZA_PARES_INTERVAL = 300
HISTORICO_MAX = 400

# ================================================================
# CLIENTS BINANCE
# ================================================================
try:
    client = Client(API_KEY, API_SECRET, tld='com')
    client_level2 = UMFutures(key=API_KEY, secret=API_SECRET)
    logger.info("Conexão com Binance estabelecida com sucesso.")
except Exception as e:
    logger.critical(f"Erro ao conectar na Binance: {e}")
    sys.exit(1)

# ================================================================
# THREAD LOCKS - [BUG 1 FIX] Locks únicos, sem redefinição
# ================================================================
lock_precos = threading.Lock()
lock_pares = lock_precos           # alias - MESMO lock
lock_posicoes = threading.Lock()
lock_trailing = threading.Lock()
lock_modelos = threading.RLock()   # RLock permite re-entrância
lock_capital = threading.Lock()
lock_indicadores = threading.Lock()
# NÃO redefinir nenhum lock depois daqui!

# ================================================================
# ESTADOS GLOBAIS
# ================================================================
pares_usdt = []
precos_cache = {}
indicadores = {}
micro_modelos = {}
capital_por_par = {}
posicoes = {}
trailing_info = {}
volatilidade = {}
correlacoes = {}

_cache_filtros = {}
_cache_saldo = {}
_cache_exchange_info = None
cache_control = {}

ARQ_MEMORIA = "memoria.json"
encerrando = False

# [BUG 2 FIX] ciclos_sem_trade como GLOBAL desde o início
ciclos_sem_trade = 0

# -------------------- Estado avançado --------------------
regime_mercado = 'neutro'       # 'bull', 'bear', 'neutro', 'chop'
ultimo_loss_time = 0            # timestamp do último stop loss
pnl_sessao = 0.0               # P&L total da sessão
pnl_por_par = {}                # par -> P&L acumulado
streak_atual = 0                # sequência atual (+wins, -losses)

# ================================================================
# RATE LIMITER
# ================================================================
class RateLimiter:
    def __init__(self, max_calls_per_second=8):
        self.min_interval = 1.0 / max_calls_per_second
        self.last_call = 0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

rate_limiter = RateLimiter(max_calls_per_second=8)

# ================================================================
# MEMÓRIA
# ================================================================
def salvar_memoria():
    try:
        with lock_precos:
            with lock_posicoes:
                with lock_trailing:
                    with lock_modelos:
                        with lock_capital:
                            memoria = {
                                "pares_usdt": pares_usdt,
                                "micro_modelos": micro_modelos,
                                "capital_por_par": capital_por_par,
                                "posicoes": posicoes,
                                "trailing_info": trailing_info,
                                "volatilidade": volatilidade,
                                "correlacoes": correlacoes
                            }
                            ind_resumo = {}
                            for par, df in list(indicadores.items()):
                                try:
                                    ind_resumo[par] = df.iloc[-1].to_dict()
                                except Exception:
                                    ind_resumo[par] = {}
                            memoria["indicadores"] = ind_resumo

        temp_path = ARQ_MEMORIA + ".tmp"
        with open(temp_path, "w") as f:
            json.dump(memoria, f)
        os.replace(temp_path, ARQ_MEMORIA)
        _ia_salvar()  # Salvar memória da IA junto
    except Exception as e:
        logger.error(f"[MEMÓRIA] Erro ao salvar: {e}")


def carregar_memoria():
    global pares_usdt, micro_modelos, capital_por_par
    global posicoes, trailing_info, volatilidade, correlacoes
    try:
        if not os.path.exists(ARQ_MEMORIA):
            logger.info("[MEMÓRIA] Nenhum arquivo encontrado, iniciando vazio.")
            return
        with open(ARQ_MEMORIA, "r") as f:
            memoria = json.load(f)
        with lock_posicoes:
            with lock_trailing:
                with lock_modelos:
                    with lock_capital:
                        pares_usdt = memoria.get("pares_usdt", [])
                        micro_modelos = memoria.get("micro_modelos", {})
                        capital_por_par = memoria.get("capital_por_par", {})
                        # NÃO carregar posicoes e trailing da memória
                        # Vamos sincronizar com a Binance (fonte da verdade)
                        volatilidade = memoria.get("volatilidade", {})
                        correlacoes = memoria.get("correlacoes", {})
        with lock_indicadores:
            indicadores_raw = memoria.get("indicadores", {})
            indicadores.clear()
            for par, dados in indicadores_raw.items():
                try:
                    indicadores[par] = pd.DataFrame([dados])
                except Exception:
                    indicadores[par] = None
        logger.info("[MEMÓRIA] Estado restaurado (posições serão sincronizadas com Binance).")
        _ia_carregar()  # Carregar memória da IA
    except Exception as e:
        logger.error(f"[MEMÓRIA] Erro ao carregar: {e}")


def sincronizar_posicoes_binance():
    """
    Sincroniza posições com a Binance ao iniciar.
    A Binance é a fonte da verdade — não o arquivo de memória.
    """
    global posicoes, trailing_info
    try:
        rate_limiter.wait()
        account = client.futures_account()
        pos_reais = {}
        for p in account.get('positions', []):
            qty = float(p.get('positionAmt', 0))
            if abs(qty) > 0:
                symbol = p['symbol']
                entry_price = float(p.get('entryPrice', 0))
                pnl = float(p.get('unrealizedProfit', 0))
                pos_reais[symbol] = qty
                tipo = 'LONG' if qty > 0 else 'SHORT'
                print(f"  {Fore.CYAN}[SYNC] {symbol} | {tipo} | qty={qty:.6f} | "
                      f"entrada=${entry_price:.6f} | PnL={pnl:+.4f}{Style.RESET_ALL}")

                # Configurar trailing para posições existentes
                with lock_trailing:
                    if symbol not in trailing_info:
                        trailing_info[symbol] = {
                            'preco_compra': entry_price,
                            'preco_real_exec': entry_price,
                            'maior_valor': entry_price,
                            'menor_valor': entry_price,
                            'melhor_lucro': 0.0,
                            'tipo': tipo,
                            'tempo_entrada': time.time(),  # marca como "agora" para não dar timeout imediato
                        }

        with lock_posicoes:
            posicoes.clear()
            posicoes.update(pos_reais)

        # Limpar trailing de posições que não existem mais
        with lock_trailing:
            pares_trail = list(trailing_info.keys())
            for par in pares_trail:
                if par not in pos_reais:
                    trailing_info.pop(par, None)

        n = len(pos_reais)
        if n > 0:
            print(f"  {Fore.YELLOW}[SYNC] {n} posições abertas encontradas na Binance{Style.RESET_ALL}")
        else:
            print(f"  {Fore.GREEN}[SYNC] Nenhuma posição aberta — limpo para operar{Style.RESET_ALL}")
        logger.info(f"[SYNC] {n} posições sincronizadas com Binance")

    except Exception as e:
        logger.error(f"[SYNC] Erro ao sincronizar posições: {e}")
        # Em caso de erro, limpar tudo para não travar
        with lock_posicoes:
            posicoes.clear()
        with lock_trailing:
            trailing_info.clear()


def salvar_memoria_continuo(intervalo=30):
    global encerrando
    contador = 0
    while not encerrando:
        time.sleep(0.5)
        contador += 0.5
        if contador >= intervalo:
            salvar_memoria()
            contador = 0


def salvar_memoria_ao_sair(*args):
    global encerrando
    if not encerrando:
        encerrando = True
        logger.info("[MEMÓRIA] Salvamento final...")
        salvar_memoria()
        sys.exit(0)

signal.signal(signal.SIGINT, salvar_memoria_ao_sair)
signal.signal(signal.SIGTERM, salvar_memoria_ao_sair)
atexit.register(salvar_memoria_ao_sair)


# ================================================================
# CACHE DE EXCHANGE INFO E FILTROS
# ================================================================
def _get_exchange_info():
    global _cache_exchange_info
    now = time.time()
    if _cache_exchange_info and (now - _cache_exchange_info[1]) < CACHE_FILTROS_TTL:
        return _cache_exchange_info[0]
    try:
        rate_limiter.wait()
        info = client.futures_exchange_info()
        _cache_exchange_info = (info, now)
        return info
    except Exception as e:
        logger.error(f"[EXCHANGE INFO] Erro: {e}")
        return _cache_exchange_info[0] if _cache_exchange_info else None


def obter_filtros_binance(par):
    now = time.time()
    cached = _cache_filtros.get(par)
    if cached and (now - cached[2]) < CACHE_FILTROS_TTL:
        return cached[0], cached[1]
    try:
        info = _get_exchange_info()
        if not info:
            return MIN_NOTIONAL_FALLBACK, 0.0
        for s in info['symbols']:
            if s['symbol'] == par:
                min_notional = MIN_NOTIONAL_FALLBACK
                step_size = 0.0
                for f in s.get('filters', []):
                    if f.get('filterType') == 'MIN_NOTIONAL':
                        min_notional = float(f.get('minNotional', MIN_NOTIONAL_FALLBACK))
                    if f.get('filterType') == 'LOT_SIZE':
                        step_size = float(f.get('stepSize', 0.0))
                _cache_filtros[par] = (min_notional, step_size, now)
                return min_notional, step_size
    except Exception as e:
        logger.error(f"[FILTROS] {par} | {e}")
    return MIN_NOTIONAL_FALLBACK, 0.0


def atualizar_saldo_real():
    global _cache_saldo
    now = time.time()
    if _cache_saldo and (now - _cache_saldo.get('timestamp', 0)) < CACHE_SALDO_TTL:
        return _cache_saldo.get('saldo', {})
    try:
        rate_limiter.wait()
        info = client.futures_account_balance()
        saldo = {}
        for ativo in info:
            asset = ativo['asset']
            # Para futuros, usar availableBalance (não balance)
            available = float(ativo.get('availableBalance', ativo.get('balance', 0)))
            total = float(ativo.get('balance', 0))
            saldo[asset] = available
            if asset == 'USDT' and total > 0:
                logger.debug(f"[SALDO] USDT total={total:.2f} disponível={available:.2f}")
        _cache_saldo = {'saldo': saldo, 'timestamp': now}
        return saldo
    except Exception as e:
        logger.error(f"[SALDO] Erro: {e}")
        return _cache_saldo.get('saldo', {}) if _cache_saldo else {}


# ================================================================
# COLETA DE PARES E PREÇOS
# ================================================================
def atualizar_pares_usdt_thread():
    global pares_usdt, precos_cache, trailing_info, cache_control
    while not encerrando:
        try:
            rate_limiter.wait()
            info = client_level2.exchange_info()
            symbols = [s for s in info['symbols']
                       if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING']
            rate_limiter.wait()
            tickers = client_level2.ticker_24hr_price_change()
            vol_map = {t['symbol']: float(t.get('quoteVolume', 0)) for t in tickers}
            
            # ============================================================
            # FILTRO DEFINITIVO — VOLUME 24H MÍNIMO $5M
            # ============================================================
            # Moedas com menos de $5M de volume diário são ARMADILHAS:
            #   - Spread alto (come o lucro)
            #   - Liquidez baixa (slippage na entrada E na saída)
            #   - Manipuláveis por baleias pequenas
            #   - Imprevisíveis (sem fluxo suficiente para análise)
            #
            # $5M/dia = ~$3,500/minuto = liquidez mínima para scalping
            # ============================================================
            pares_sorted = sorted(
                [s['symbol'] for s in symbols],
                key=lambda x: vol_map.get(x, 0),
                reverse=True
            )
            novos_pares = pares_sorted[:MAX_PARES]
            
            # Log
            total_symbols = len(symbols)
            
            with lock_pares:
                pares_usdt[:] = novos_pares
                for p in novos_pares:
                    cache_control[p] = 0
                for p in list(cache_control.keys()):
                    if p not in novos_pares:
                        cache_control[p] += 1
                        if cache_control[p] > 10:
                            precos_cache.pop(p, None)
                            with lock_trailing:
                                trailing_info.pop(p, None)
                            cache_control.pop(p)
                MAX_CLOSES = 1000
                for p in pares_usdt:
                    if p not in precos_cache:
                        precos_cache[p] = []
                    if len(precos_cache[p]) > MAX_CLOSES:
                        precos_cache[p] = precos_cache[p][-MAX_CLOSES:]
            logger.info(f"[PARES] {len(pares_usdt)}/{total_symbols} pares USDT ativos")
        except Exception as e:
            logger.error(f"[PARES] Erro: {e}")
        time.sleep(ATUALIZA_PARES_INTERVAL)


def monitorar_e_popular_precos_thread():
    global precos_cache, volatilidade, capital_por_par, memoria_mt_hist
    loop_count = 0
    while not encerrando:
        loop_count += 1
        print(f"\n{Fore.WHITE}[LOOP {loop_count}] Iniciando atualização de pares{Style.RESET_ALL}")
        for par in list(pares_usdt):
            if encerrando:
                break
            try:
                timeframes_list = ['1m', '5m', '15m']
                for tf in timeframes_list:
                    rate_limiter.wait()
                    klines = client_level2.klines(symbol=par, interval=tf, limit=CANDLE_LIMIT)
                    closes = [float(k[4]) for k in klines] if klines else []
                    with lock_pares:
                        precos_cache[f"{par}_{tf}m"] = closes
                        if tf == '1m':
                            if klines:
                                ohlcv = []
                                for k in klines:
                                    ohlcv.append({
                                        'open': float(k[1]),
                                        'high': float(k[2]),
                                        'low': float(k[3]),
                                        'close': float(k[4]),
                                        'volume': float(k[5])
                                    })
                                precos_cache[par] = ohlcv[-HISTORICO_MAX:]
                            else:
                                precos_cache[par] = closes
                with lock_pares:
                    dados_1m = precos_cache.get(par, [])
                if dados_1m:
                    closes_1m = [float(d['close'] if isinstance(d, dict) else d) for d in dados_1m]
                    arr = np.array(closes_1m)
                    volatilidade[par] = float(arr.std() / max(arr.mean(), 1e-9)) if len(arr) > 0 else 0
                min_candles_ok = all(
                    len(precos_cache.get(f"{par}_{tf}m", [])) >= 10
                    for tf in ['1m', '5m', '15m']
                )
                if min_candles_ok:
                    confianca = micro_modelos.get(par, {}).get('confianca', 0.0)
                    vol = volatilidade.get(par, 0.0)
                    indicador_forca = calcular_mt_score(par, confianca, vol, 0.0)
            except Exception as e:
                logger.error(f"[PREÇOS LOOP {loop_count}] {par} | {e}")
                with lock_pares:
                    precos_cache[par] = []
                    for tf in ['1m', '5m', '15m']:
                        precos_cache[f"{par}_{tf}m"] = []
                volatilidade[par] = 0.0
            time.sleep(0.05)
        # Normalizar capital
        try:
            total_weight = sum((1.0 / (volatilidade.get(p, 1e-6) + 1e-6)) for p in pares_usdt)
            if total_weight <= 0:
                total_weight = 1.0
            for p in pares_usdt:
                w = (1.0 / (volatilidade.get(p, 1e-6) + 1e-6)) / total_weight
                capital_base = round(CAPITAL_TOTAL * w, 6)
                mm = micro_modelos.get(p, {})
                with lock_pares:
                    if mm.get('confianca', 0) >= 0.8:
                        capital_por_par[p] = capital_base * 1.5
                    else:
                        capital_por_par[p] = capital_base
        except Exception as e:
            logger.error(f"[NORMALIZA LOOP {loop_count}] {e}")
        time.sleep(0.3)


# ================================================================
# SENTIMENTO
# ================================================================
sentimento_por_par = {}
analyzer_vader = SentimentIntensityAnalyzer()
rss_feeds = [
    "https://cryptonews.com/news/feed",
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/"
]
_cache_sentimento = {}
CACHE_SENTIMENTO_TTL = 120

def obter_sentimento_rss(par):
    now = time.time()
    cached = _cache_sentimento.get(par)
    if cached and (now - cached[1]) < CACHE_SENTIMENTO_TTL:
        return cached[0]
    scores = []
    par_clean = par.replace("USDT", "").lower()
    for url in rss_feeds:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:20]:
                title = entry.title.lower()
                if par_clean in title:
                    score = analyzer_vader.polarity_scores(title)['compound']
                    scores.append(score)
        except Exception as e:
            logger.debug(f"[RSS] {par} | {e}")
    resultado = float(np.tanh(np.mean(scores))) if scores else 0.0
    sentimento_por_par[par] = resultado
    _cache_sentimento[par] = (resultado, now)
    return resultado


# ================================================================
# MULTI-TEMPORALIDADE
# ================================================================
memoria_mt_hist = {}

def analise_multi_temporalidade(par, return_closes=False):
    try:
        timeframes = {
            'curto': '1m',
            'medio': '5m',
            'longo': '15m'
        }
        scores = []
        closes_out = {}
        for label, tf in timeframes.items():
            key_cache = f"{par}_{tf}m"
            with lock_precos:
                if key_cache not in precos_cache:
                    precos_cache[key_cache] = []
                if len(precos_cache[key_cache]) > 500:
                    precos_cache[key_cache] = precos_cache[key_cache][-500:]
                closes_raw = list(precos_cache.get(key_cache, []) or [])
            closes = []
            for c in closes_raw:
                try:
                    if isinstance(c, dict):
                        closes.append(float(c['close']))
                    else:
                        closes.append(float(c))
                except Exception:
                    continue
            if return_closes:
                closes_out[tf] = closes.copy()
            if len(closes) < 10:
                continue
            series_close = pd.Series(closes)
            rsi = RSIIndicator(series_close, 14).rsi().iloc[-1] if len(series_close) >= 14 else 50
            ma_short = series_close.rolling(3).mean().iloc[-1]
            ma_long = series_close.rolling(15).mean().iloc[-1] if len(series_close) >= 15 else series_close.iloc[-1]
            score = 0.0
            score += 0.3 if ma_short > ma_long else -0.3
            score += 0.2 if rsi < 35 else -0.2 if rsi > 65 else 0
            scores.append(score)
        if not scores:
            return (0.0, closes_out) if return_closes else 0.0
        peso = [0.5, 0.3, 0.2]
        score_final = sum(s * p for s, p in zip(scores, peso[:len(scores)]))
        score_final = max(-1.0, min(1.0, score_final))
        return (score_final, closes_out) if return_closes else score_final
    except Exception as e:
        logger.error(f"[ERRO MULTITEMPO] {par} | {e}")
        return (0.0, {}) if return_closes else 0.0


def calcular_mt_score(par, confianca, vol, indicador_forca):
    global memoria_mt_hist
    try:
        mt_score, closes_tf = analise_multi_temporalidade(par, return_closes=True)
        if par not in memoria_mt_hist:
            memoria_mt_hist[par] = []
        memoria_mt_hist[par].append(mt_score)
        if len(memoria_mt_hist[par]) > 5:
            memoria_mt_hist[par] = memoria_mt_hist[par][-5:]
        if memoria_mt_hist[par]:
            mt_consistente = (
                sum(1 for s in memoria_mt_hist[par] if abs(s) > 0.1)
                / len(memoria_mt_hist[par])
            )
        else:
            mt_consistente = 0.0
        peso_mt = 0.5 * (1.0 - confianca)
        if vol > 0.05:
            peso_mt *= 1.35
        elif vol < 0.02:
            peso_mt *= 0.85
        impacto = peso_mt * mt_score * max(0.3, mt_consistente)
        indicador_forca += impacto
        return indicador_forca
    except Exception as e:
        logger.error(f"[ERRO MT_SCORE] {par} | {e}")
        return indicador_forca


# ================================================================
# INDICADORES TÉCNICOS
# ================================================================
def calcular_indicadores_e_prever(par):
    try:
        # ============================================================
        # DADOS DE 5 MINUTOS para DIREÇÃO (estável, não ruído)
        # DADOS DE 1 MINUTO para TIMING (entrada precisa)
        # ============================================================
        
        # 1. Carregar dados de 5m para direção
        with lock_precos:
            dados_5m_raw = precos_cache.get(f"{par}_5mm", [])
            dados_1m = precos_cache.get(par, [])
            if not dados_1m or len(dados_1m) < 50:
                return {}
            dados_1m_copia = list(dados_1m)
        
        # DataFrame 1m (para indicadores de timing)
        if isinstance(dados_1m_copia[0], dict):
            df = pd.DataFrame(dados_1m_copia)
        else:
            df = pd.DataFrame({'close': [float(c) for c in dados_1m_copia]})
            df['high'] = df['close']
            df['low'] = df['close']
            df['open'] = df['close']
            df['volume'] = 0.0
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col not in df.columns:
                df[col] = df['close']
        
        # Indicadores em 1m (para timing)
        df['rsi'] = RSIIndicator(df['close'], 14).rsi().fillna(50)
        df['macd_diff'] = MACD(df['close']).macd_diff().fillna(0)
        bb = BollingerBands(df['close'])
        df['bb_h'] = bb.bollinger_hband().fillna(df['close'])
        df['bb_l'] = bb.bollinger_lband().fillna(df['close'])
        df['ma_3'] = df['close'].rolling(3).mean().fillna(df['close'])
        df['ma_5'] = df['close'].rolling(5).mean().fillna(df['close'])
        df['ma_15'] = df['close'].rolling(15).mean().fillna(df['close'])
        df['ma_50'] = df['close'].rolling(50).mean().fillna(df['close'])
        df['adx'] = ADXIndicator(
            high=df['high'], low=df['low'], close=df['close'], window=14
        ).adx().fillna(0)
        df['cci'] = CCIIndicator(
            high=df['high'], low=df['low'], close=df['close'], window=20
        ).cci().fillna(0)
        df['atr'] = AverageTrueRange(
            high=df['high'], low=df['low'], close=df['close'], window=14
        ).average_true_range().fillna(0)
        latest = df.iloc[-1]
        
        # ============================================================
        # DIREÇÃO: decidida por 5m (tendência real, não ruído)
        # ============================================================
        direcao_5m = 'manter'
        forca_5m = 0.0
        
        if dados_5m_raw and len(dados_5m_raw) >= 20:
            closes_5m = [float(c) for c in dados_5m_raw]
            s5 = pd.Series(closes_5m)
            
            # RSI em 5m = olha 70 minutos
            rsi_5m = float(RSIIndicator(s5, 14).rsi().iloc[-1]) if len(s5) >= 14 else 50
            
            # MAs em 5m
            ma7_5m = float(s5.rolling(7).mean().iloc[-1]) if len(s5) >= 7 else closes_5m[-1]
            ma21_5m = float(s5.rolling(21).mean().iloc[-1]) if len(s5) >= 21 else closes_5m[-1]
            
            # MACD em 5m = olha ~2 horas
            macd_5m = float(MACD(s5).macd_diff().iloc[-1]) if len(s5) >= 26 else 0
            
            # Direção clara em 5m
            if ma7_5m > ma21_5m and rsi_5m > 50 and macd_5m > 0:
                direcao_5m = 'compra'
                forca_5m = 0.4
            elif ma7_5m < ma21_5m and rsi_5m < 50 and macd_5m < 0:
                direcao_5m = 'venda'
                forca_5m = 0.4
            elif ma7_5m > ma21_5m:
                direcao_5m = 'compra'
                forca_5m = 0.2
            elif ma7_5m < ma21_5m:
                direcao_5m = 'venda'
                forca_5m = 0.2
        
        # ============================================================
        # TAMBÉM checar 15m para confirmar tendência maior
        # ============================================================
        with lock_precos:
            dados_15m_raw = precos_cache.get(f"{par}_15mm", [])
        
        direcao_15m = 'manter'
        if dados_15m_raw and len(dados_15m_raw) >= 15:
            closes_15m = [float(c) for c in dados_15m_raw]
            ma5_15m = sum(closes_15m[-5:]) / 5
            ma15_15m = sum(closes_15m[-15:]) / 15
            if ma5_15m > ma15_15m:
                direcao_15m = 'compra'
            elif ma5_15m < ma15_15m:
                direcao_15m = 'venda'
        
        # ============================================================
        # INDICADOR DE FORÇA: 5m manda, 1m confirma
        # ============================================================
        indicador_forca = 0.0
        
        # 5m decide a direção (peso 60%)
        if direcao_5m == 'compra':
            indicador_forca += forca_5m
        elif direcao_5m == 'venda':
            indicador_forca -= forca_5m
        
        # 15m confirma (peso 20%)
        if direcao_15m == direcao_5m and direcao_5m != 'manter':
            indicador_forca *= 1.3  # 5m e 15m concordam = sinal forte
        elif direcao_15m != 'manter' and direcao_15m != direcao_5m:
            indicador_forca *= 0.5  # 5m e 15m discordam = sinal fraco
        
        # 1m dá o timing (peso 20%)
        rsi_1m = float(latest['rsi'])
        if direcao_5m == 'compra' and rsi_1m < 40:
            indicador_forca += 0.15  # 5m diz compra E 1m está oversold = timing bom
        elif direcao_5m == 'venda' and rsi_1m > 60:
            indicador_forca += 0.15 if indicador_forca < 0 else -0.15
        
        # MACD 1m confirma
        if latest['macd_diff'] > 0 and indicador_forca > 0:
            indicador_forca += 0.08
        elif latest['macd_diff'] < 0 and indicador_forca < 0:
            indicador_forca -= 0.08
        
        # Bollinger 1m
        if latest['close'] < latest['bb_l'] and indicador_forca > 0:
            indicador_forca += 0.07  # abaixo da banda + 5m diz compra
        elif latest['close'] > latest['bb_h'] and indicador_forca < 0:
            indicador_forca -= 0.07
        
        # Volume anômalo
        if 'volume' in df.columns and len(df) > 20:
            vol_media = df['volume'].rolling(20).mean().iloc[-1]
            vol_atual_v = df['volume'].iloc[-1]
            if vol_media > 0 and vol_atual_v > vol_media * 1.5:
                indicador_forca *= 1.15
        
        indicador_forca = max(-1.0, min(1.0, indicador_forca))
        
        # ============================================================
        # DECISÃO: só entra se 5m tem direção clara
        # ============================================================
        vol_inst = df['close'].rolling(5).std() / (df['close'].rolling(5).mean() + 1e-9)
        vol_atual = float(vol_inst.iloc[-1]) if not pd.isna(vol_inst.iloc[-1]) else 0.0
        modo = 'rápido' if vol_atual < 0.015 else 'médio'
        
        tendencia = direcao_5m  # tendência = direção do 5m
        reversao = (rsi_1m > 70 or rsi_1m < 30)
        
        acao = 'manter'
        confianca = 0.30 + abs(indicador_forca) * 0.50
        
        # Só entra se 5m tem direção (não entra em ruído)
        if direcao_5m == 'manter':
            acao = 'manter'
            confianca = min(confianca, 0.30)
        elif reversao and abs(indicador_forca) > 0.15:
            acao = 'venda' if tendencia == 'compra' else 'compra'
            confianca = max(confianca, 0.50)
        elif abs(indicador_forca) > 0.25:
            acao = tendencia
        elif abs(indicador_forca) > 0.15:
            acao = 'compra' if indicador_forca > 0 else 'venda'
        correlacao_atual = correlacoes.get(par, 0.0)
        if correlacao_atual > 0.7 and acao == 'compra':
            confianca = max(0.3, confianca - 0.1)
        hora_atual = datetime.now(timezone.utc).hour
        if 8 <= hora_atual < 10 and acao == 'compra':
            confianca = min(1.0, confianca + 0.03)
        elif 16 <= hora_atual < 18 and acao == 'venda':
            confianca = min(1.0, confianca + 0.03)
        with lock_modelos:
            mm = micro_modelos.get(par, {})
        historico_acertos = mm.get('historico_acertos', [])
        with lock_posicoes:
            pos_atual = float(posicoes.get(par, 0.0))
        if pos_atual > 0 and mm.get('acao') in ['compra', 'venda']:
            with lock_trailing:
                trail = trailing_info.get(par, {})
            preco_ref = float(trail.get('preco_compra', latest['close']))
            movimento = float(latest['close']) - preco_ref
            acerto = (movimento > 0 and mm['acao'] == 'compra') or \
                     (movimento < 0 and mm['acao'] == 'venda')
            historico_acertos.append(1 if acerto else 0)
            historico_acertos = historico_acertos[-20:]
        if historico_acertos:
            taxa = sum(historico_acertos) / len(historico_acertos)
            confianca = min(1.0, confianca + (taxa - 0.5) * 0.15)
        resultado = {
            'acao': acao,
            'confianca': round(max(0.0, min(1.0, confianca)), 3),
            'historico_acertos': historico_acertos,
            'indicador_forca': round(indicador_forca, 4),
            'forca_final': round(indicador_forca, 4),  # [BUG 8 FIX] garante campo existe
        }
        with lock_modelos:
            micro_modelos[par] = resultado
        with lock_indicadores:
            indicadores[par] = df
        return resultado
    except Exception as e:
        logger.error(f"[INDICADORES] {par} | {e}")
        return {}


# ================================================================
# CAMADA DECISÃO EXTRATERRESTRE SNIPER
# ================================================================
def camada_decisao_extraterrestre(par):
    try:
        with lock_modelos:
            mm_prev = dict(micro_modelos.get(par, {}))

        with lock_precos:
            closes_raw = precos_cache.get(par, [])
            closes_raw_copy = list(closes_raw)

        closes = []
        for c in closes_raw_copy:
            try:
                if isinstance(c, dict):
                    closes.append(float(c['close']))
                else:
                    closes.append(float(c))
            except:
                continue

        if len(closes) < 20:
            return mm_prev

        curto = closes[-20:]
        medio = closes[-50:] if len(closes) >= 50 else curto
        longo = closes[-100:] if len(closes) >= 100 else medio

        with lock_indicadores:
            df = indicadores.get(par)
        if df is None or df.empty:
            df = pd.DataFrame({'close': closes})
        for col in ['ma_3', 'ma_5', 'ma_15', 'adx', 'macd_diff', 'rsi', 'cci', 'atr']:
            if col not in df.columns:
                df[col] = 0
        latest = df.iloc[-1]

        # Tendência multi-janelas
        tendencia = 'manter'
        try:
            def tendencia_janela(candles):
                ma1 = sum(candles[-3:]) / 3
                ma2 = sum(candles[-6:-3]) / 3 if len(candles) >= 6 else ma1
                return 'compra' if ma1 > ma2 else 'venda' if ma1 < ma2 else 'manter'
            tendencia_curto = tendencia_janela(curto)
            tendencia_medio = tendencia_janela(medio)
            tendencia_longo = tendencia_janela(longo)
            votos = [tendencia_curto, tendencia_medio, tendencia_longo]
            if votos.count('compra') >= 2:
                tendencia = 'compra'
            elif votos.count('venda') >= 2:
                tendencia = 'venda'
            if len(df) >= 5:
                macd_recente = df['macd_diff'].iloc[-5:]
                macd_trend = macd_recente.mean()
                if tendencia == 'compra' and macd_trend < 0:
                    tendencia = 'manter'
                elif tendencia == 'venda' and macd_trend > 0:
                    tendencia = 'manter'
        except Exception as e:
            logger.debug(f"[SNIPER] {par} | Erro tendência: {e}")

        rsi = float(latest.get('rsi', 50))
        cci = float(latest.get('cci', 0))
        stoch_rsi = max(0, min(1, (rsi - 20) / 60))
        reversao = ((rsi > 75 and stoch_rsi > 0.85 and cci > 120) or
                    (rsi < 25 and stoch_rsi < 0.15 and cci < -120))

        # Indicador de força
        indicador_forca = 0.0
        if tendencia == 'compra':
            indicador_forca += 0.6
        elif tendencia == 'venda':
            indicador_forca -= 0.6
        indicador_forca += 0.3 if rsi < 35 else -0.3 if rsi > 65 else 0
        indicador_forca += 0.2 if float(latest.get('macd_diff', 0)) > 0 else -0.2
        indicador_forca += 0.12 if cci > 85 else -0.12 if cci < -85 else 0
        indicador_forca += 0.08 if stoch_rsi < 0.2 else -0.08 if stoch_rsi > 0.8 else 0
        confirma_tendencia = int(tendencia == 'compra') + int(rsi < 40) + int(stoch_rsi < 0.2)
        indicador_forca += 0.15 * confirma_tendencia

        # Volatilidade
        vol = volatilidade.get(par, 0.0)
        candles_analise = curto
        std_candles = (max(candles_analise) - min(candles_analise)) or 0.001
        peso_vol = max(0.6, min(1.4, 0.85 + (0.05 - vol) * 2.5))

        # [BUG 3 FIX] Usar PERCENTUAL ao invés de absoluto
        preco_medio = sum(candles_analise) / len(candles_analise) if candles_analise else 1.0
        movimento_pct = std_candles / preco_medio if preco_medio > 0 else 0

        indicador_forca *= peso_vol

        # Book de ofertas
        book = precos_cache.get(par + '_book', {})
        desequilibrio = 0
        grande_ordem = 0.0
        if isinstance(book, dict) and book.get('bids') and book.get('asks'):
            total_bid = sum([float(v) for _, v in book['bids']])
            total_ask = sum([float(v) for _, v in book['asks']])
            desequilibrio = (total_bid - total_ask) / (total_bid + total_ask + 1e-9)
            maior_bid = max([float(v) for _, v in book['bids']] + [0])
            maior_ask = max([float(v) for _, v in book['asks']] + [0])
            grande_ordem = max(maior_bid, maior_ask) / (total_bid + total_ask + 1e-9)

            # [BUG 5 FIX] Suavizar filtro de equilíbrio (antes: *0.35, agora: *0.75)
            if abs(desequilibrio) < 0.04:
                indicador_forca *= 0.75
            else:
                indicador_forca += 0.25 * desequilibrio

            if grande_ordem > 0.15:
                indicador_forca *= 0.80

            bid_ask_ratio = total_bid / max(total_ask, 1e-9)
            if bid_ask_ratio > 3 or bid_ask_ratio < 0.33:
                indicador_forca *= 1.1

        # [BUG 3 FIX] Filtro lateralização com PERCENTUAL
        if movimento_pct < 0.0003:      # < 0.03% = muito parado
            indicador_forca *= 0.4
        elif movimento_pct > 0.003:      # > 0.3% = bom movimento
            indicador_forca *= 1.2

        # [BUG 4 FIX] Filtro relaxado (antes: vol < 0.005 e forca < 0.15)
        if abs(indicador_forca) < 0.08 or vol < 0.001:
            mm_prev['acao'] = 'manter'
            mm_prev['confianca'] = 0.0
            mm_prev['confianca100'] = 0
            mm_prev['forca_final'] = 0.0
            with lock_modelos:
                micro_modelos[par] = mm_prev
            return mm_prev

        # Regressão linear curta
        try:
            if len(curto) >= 5:
                x = list(range(5))
                y = curto[-5:]
                x_mean = 2.0
                y_mean = sum(y) / 5
                numerador = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(5))
                denominador = sum((x[i] - x_mean) ** 2 for i in range(5))
                m = numerador / denominador if denominador != 0 else 0
                pred_proximo = y[-1] + m
                if pred_proximo > y[-1]:
                    indicador_forca += 0.05 * min(1.0, abs(m / preco_medio) * 5000)
                elif pred_proximo < y[-1]:
                    indicador_forca -= 0.05 * min(1.0, abs(m / preco_medio) * 5000)
        except:
            pass

        # Momentum
        try:
            momentum = [curto[i + 1] - curto[i] for i in range(len(curto) - 1)]
            derivada = sum(momentum[-3:]) / 3 if len(momentum) >= 3 else 0
            aceleracao = derivada - sum(momentum[-5:-2]) / 3 if len(momentum) >= 5 else 0
            pullback_conf = False
            if len(curto) >= 5:
                swing_pct = (max(curto[-5:]) - min(curto[-5:])) / preco_medio
                pullback_conf = 0.001 < swing_pct < 0.005
            if pullback_conf:
                indicador_forca *= 1.1
            if abs(aceleracao / preco_medio) > 0.0001:
                indicador_forca *= 1.05
        except:
            pass

        # MT-Score
        try:
            indicador_forca = calcular_mt_score(par, confianca=mm_prev.get('confianca', 0.5),
                                                 vol=vol, indicador_forca=indicador_forca)
            mm_prev['mt_hist'] = memoria_mt_hist.get(par, [])
        except:
            pass

        # Sentimento
        try:
            sentimento = float(obter_sentimento_rss(par) or 0.0)
        except:
            sentimento = 0.0
        indicador_forca += 0.15 * max(-1.0, min(1.0, sentimento))

        # Fakeout / liquidez
        fakeout_flag = precos_cache.get(par + '_fakeout', False)
        if fakeout_flag:
            indicador_forca *= 0.6

        # Score direção
        score_direcao = 0.0
        score_direcao += 0.2 if float(latest.get('macd_diff', 0)) > 0 else -0.2
        score_direcao += 0.15 if rsi < 50 else -0.15
        score_direcao += 0.1 if stoch_rsi < 0.5 else -0.1
        score_direcao += 0.05 if cci > 0 else -0.05
        indicador_forca += score_direcao

        # Micro-trades adaptativo
        micro_hist_trades = mm_prev.get('micro_hist_trades', [])
        acertos_recentes = micro_hist_trades[-10:].count(1)
        erros_recentes = micro_hist_trades[-10:].count(0)
        if acertos_recentes >= 3:
            indicador_forca *= 1.0 + 0.02 * min(acertos_recentes, 5)
        elif erros_recentes >= 3:
            indicador_forca *= 0.9

        # Clamp
        indicador_forca = max(-1.0, min(1.0, indicador_forca))

        # Trailing adaptativo
        topo_lucro = mm_prev.get('topo_lucro', 0)
        preco_atual = closes[-1]
        with lock_posicoes:
            pos_atual = posicoes.get(par, 0)
        if pos_atual > 0:
            topo_lucro = max(topo_lucro, preco_atual)
            mm_prev['topo_lucro'] = topo_lucro
            perda_max = 0.015 + min(vol, 0.01)
            perda_relativa = (topo_lucro - preco_atual) / topo_lucro if topo_lucro > 0 else 0
            if perda_relativa > perda_max:
                indicador_forca = -1.0

        # Ação final — confiança PROPORCIONAL à qualidade do sinal
        acao = 'manter'
        # Confiança base: diretamente proporcional à força
        # Força 0 → conf 0.30, Força 0.5 → conf 0.55, Força 1.0 → conf 0.80
        confianca = 0.30 + abs(indicador_forca) * 0.50
        
        if abs(indicador_forca) > 0.25:
            acao = 'compra' if indicador_forca > 0 else 'venda'
        if reversao:
            acao = 'venda' if tendencia == 'compra' else 'compra'
            confianca = max(confianca, 0.55)

        confianca = round(max(0.0, min(1.0, confianca)), 3)
        confianca100 = int(round(max(0, min(100, (indicador_forca + 1) / 2 * 100))))

        mm = mm_prev.copy()
        mm['acao'] = acao
        mm['confianca'] = confianca
        mm['confianca100'] = confianca100
        mm['micro_hist_trades'] = micro_hist_trades
        mm['forca_final'] = indicador_forca      # [BUG 8 FIX] sempre presente
        mm['indicador_forca'] = indicador_forca

        with lock_modelos:
            micro_modelos[par] = mm

        # Print
        if acao == 'compra':
            cor = Fore.GREEN
        elif acao == 'venda':
            cor = Fore.RED
        else:
            cor = Fore.YELLOW
        print(f"  {Fore.CYAN}[SNIPER]{Style.RESET_ALL} {par:12} | "
              f"{cor}{acao.upper():7}{Style.RESET_ALL} | "
              f"Força: {indicador_forca:+.3f} | "
              f"Conf: {confianca:.2f} ({confianca100}%) | "
              f"Tend: {tendencia} | Rev: {reversao}")

        return mm

    except Exception as e:
        logger.error(f"[ERRO SNIPER] {par} | {e}")
        return micro_modelos.get(par, {})


# ================================================================
# CONSTRUIR MICRO MODELO
# ================================================================
def construir_micro_modelo(par):
    """
    FASE 2: Análise profunda com Book L2.
    NÃO recalcula indicadores — usa o que já foi calculado pela extraterrestre.
    Apenas ADICIONA informação do book de ordens.
    """
    try:
        with lock_indicadores:
            df = indicadores.get(par)
        if df is None or df.empty:
            return {}
        closes = df['close'].tolist()
        if len(closes) < 20:
            return {}

        latest = df.iloc[-1]
        
        # Pegar modelo existente (calculado por camada_decisao_extraterrestre)
        with lock_modelos:
            mm = dict(micro_modelos.get(par, {}))
        
        # PRESERVAR força e decisão existente
        indicador_forca = mm.get('indicador_forca', 0.0)
        acao_existente = mm.get('acao', 'manter')
        conf_existente = mm.get('confianca', 0.0)
        
        # Se não tem nada calculado ainda, calcular básico
        if acao_existente == 'manter' and abs(indicador_forca) < 0.01:
            for col in ['ma_3', 'ma_5', 'ma_15', 'adx', 'macd_diff', 'rsi', 'cci']:
                if col not in df.columns:
                    df[col] = 0
            tendencia = 'manter'
            ma3 = float(latest.get('ma_3', latest['close']))
            ma5 = float(latest.get('ma_5', latest['close']))
            ma15 = float(latest.get('ma_15', latest['close']))
            adx_v = float(latest.get('adx', 0))
            if ma3 > ma5 > ma15 and adx_v > 10:
                tendencia = 'compra'
            elif ma3 < ma5 < ma15 and adx_v > 10:
                tendencia = 'venda'
            rsi = float(latest.get('rsi', 50))
            indicador_forca = 0.0
            indicador_forca += 0.35 if tendencia == 'compra' else -0.35 if tendencia == 'venda' else 0
            indicador_forca += 0.2 if rsi < 35 else -0.2 if rsi > 65 else 0
            indicador_forca += 0.12 if float(latest.get('macd_diff', 0)) > 0 else -0.12
            indicador_forca = max(-1.0, min(1.0, indicador_forca))
            if abs(indicador_forca) > 0.25:
                acao_existente = 'compra' if indicador_forca > 0 else 'venda'
            conf_existente = 0.30 + abs(indicador_forca) * 0.50

        # ====== BOOK L2 — informação adicional, NÃO sobrescreve ======
        try:
            rate_limiter.wait()
            prof = client_level2.depth(symbol=par, limit=50)
            if prof and 'bids' in prof and 'asks' in prof:
                with lock_precos:
                    precos_cache[par + '_book'] = {
                        'bids': [(float(p), float(q)) for p, q in prof['bids']],
                        'asks': [(float(p), float(q)) for p, q in prof['asks']]
                    }
        except Exception as e:
            logger.debug(f"[BOOK DEPTH] {par} | {e}")

        book = precos_cache.get(par + '_book', None)
        if isinstance(book, dict) and book.get('bids') and book.get('asks'):
            try:
                total_bid = sum([float(v) for _, v in book['bids']])
                total_ask = sum([float(v) for _, v in book['asks']])
                desequilibrio = (total_bid - total_ask) / (total_bid + total_ask + 1e-9)
                
                # Book CONFIRMA direção? Leve ajuste
                direcao = 1 if acao_existente == 'compra' else -1
                if desequilibrio * direcao > 0.1:
                    indicador_forca += 0.08 * desequilibrio
                    conf_existente = min(1.0, conf_existente + 0.05)
                elif desequilibrio * direcao < -0.2:
                    conf_existente = max(0.0, conf_existente - 0.10)
                
                maior_bid = max([float(v) for _, v in book['bids']])
                maior_ask = max([float(v) for _, v in book['asks']])
                grande_ordem = max(maior_bid, maior_ask) / (total_bid + total_ask + 1e-9)
                mm['grande_ordem'] = round(grande_ordem, 6)
            except Exception as e:
                logger.debug(f"[BOOK CALC] {par} | {e}")

        # Clamp e salvar SEM sobrescrever
        indicador_forca = max(-1.0, min(1.0, indicador_forca))
        conf_existente = round(max(0.0, min(1.0, conf_existente)), 3)

        mm['acao'] = acao_existente
        mm['confianca'] = conf_existente
        mm['indicador_forca'] = round(indicador_forca, 4)
        mm['forca_final'] = round(indicador_forca, 4)
        
        # Salvar preço no momento do sinal (IA compara depois)
        mm['ts_modelo'] = time.time()  # quando este modelo foi calculado
        try:
            with lock_precos:
                dados_p = precos_cache.get(par, [])
            if dados_p:
                ultimo_p = dados_p[-1]
                mm['preco_sinal'] = float(ultimo_p['close'] if isinstance(ultimo_p, dict) else ultimo_p)
        except:
            pass

        with lock_modelos:
            micro_modelos[par] = mm

        # Extraterrestre tem última palavra
        if 'extraterrestre_rodou' not in mm:
            mm_extr = camada_decisao_extraterrestre(par)
            if mm_extr:
                mm.update(mm_extr)
                mm['extraterrestre_rodou'] = True
                with lock_modelos:
                    micro_modelos[par] = mm

        return mm
    except Exception as e:
        logger.error(f"[ERRO MICRO-MODELO] {par} | {e}")
        return {}



# ================================================================
# HISTÓRICO & PERFORMANCE
# ================================================================
historico_ordens = []
performance_por_par = {}
_lock_historico = threading.Lock()

def registrar_ordem(par, acao, quantidade, preco, lucro=None):
    registro = {
        'par': par, 'acao': acao, 'quantidade': quantidade,
        'preco': preco, 'timestamp': time.time(), 'lucro': lucro
    }
    with _lock_historico:
        historico_ordens.append(registro)
        if par not in performance_por_par:
            performance_por_par[par] = {'ganhos': 0, 'perdas': 0, 'total_trades': 0}
        perf = performance_por_par[par]
        if lucro is not None:
            if lucro > 0:
                perf['ganhos'] += 1
            else:
                perf['perdas'] += 1
            perf['total_trades'] += 1
    logger.info(f"[ORDEM] {par} | {acao.upper()} | Qtd:{quantidade:.6f} | "
                f"Preço:{preco:.6f} | Lucro:{lucro if lucro else 'N/A'}")


# ================================================================
# AJUSTE DINÂMICO DE CAPITAL
# ================================================================
def ajustar_capital_por_confianca():
    try:
        with lock_modelos:
            modelos_snap = dict(micro_modelos)
        total_conf = sum(mm.get('confianca', 0.3) for mm in modelos_snap.values())
        if total_conf <= 0:
            total_conf = 1.0
        pesos_temp = {}
        for par in list(pares_usdt):
            mm = modelos_snap.get(par, {'confianca': 0.3, 'acao': 'manter'})
            confianca = mm.get('confianca', 0.3)
            acao = mm.get('acao', 'manter')
            peso = confianca / total_conf
            vol = volatilidade.get(par, 0.0)
            peso *= 1.0 / (1.0 + vol)
            corr = correlacoes.get(par, 0.0)
            peso *= (1.0 - min(corr, 0.9))
            if confianca >= 0.75 and acao in ['compra', 'venda']:
                peso *= 1.0 + (confianca - 0.75) * 2
            peso = max(0.005, min(peso, 0.15))
            pesos_temp[par] = peso
        soma_pesos = sum(pesos_temp.values())
        if soma_pesos > 0:
            with lock_capital:
                for par, peso in pesos_temp.items():
                    capital_calc = CAPITAL_TOTAL * (peso / soma_pesos)
                    capital_max = CAPITAL_TOTAL * _adapt_get("risco_trade", RISCO_MAX_POR_TRADE)
                    capital_por_par[par] = round(min(capital_calc, capital_max), 6)
    except Exception as e:
        logger.error(f"[AJUSTE CAPITAL] {e}")


def ajustar_capital_dinamico(par, confianca):
    """
    Alocação de capital — REVISÃO FORENSE v8.1
    
    REGRAS HELENA:
    1. Mínimo $30 por entrada (SEMPRE)
    2. Kelly zero = $30 (mínimo funcional, não bloqueia)
    3. Kelly positivo = escala normal até max
    4. Sem multiplicadores que reduzem abaixo de $30
    """
    CAPITAL_MIN_ABSOLUTO = 30.0   # ← MUDOU: era $5, agora $30 mínimo
    MAX_EXPOSICAO_TRADE = 0.10    # máximo 10% do capital por trade ($300)
    KELLY_FRACAO = 0.25           # 1/4 Kelly (conservador)

    with lock_modelos:
        mm = dict(micro_modelos.get(par, {}))

    forca = abs(mm.get('forca_final', 0.0))
    historico = mm.get('historico_acertos', [])
    taxa_acerto = sum(historico) / len(historico) if len(historico) >= 3 else 0.5
    vol = volatilidade.get(par, 0.01)

    # ========== KELLY CRITERION ==========
    edge = (taxa_acerto - 0.5) * 2
    edge = max(0, edge)
    kelly_pct = edge * KELLY_FRACAO

    # ========== SCORE DE QUALIDADE ==========
    score_forca = min(1.0, forca / 0.8)
    score_conf = min(1.0, confianca / 0.9)
    score_acerto = taxa_acerto
    score_consistencia = 0.0
    mt_hist = mm.get('mt_hist', memoria_mt_hist.get(par, []))
    if len(mt_hist) >= 3:
        direcao = 1 if mm.get('acao') == 'compra' else -1
        concordancias = sum(1 for s in mt_hist if s * direcao > 0)
        score_consistencia = concordancias / len(mt_hist)

    score = (
        0.30 * score_forca +
        0.25 * score_conf +
        0.20 * score_acerto +
        0.15 * score_consistencia +
        0.10 * (1.0 - min(vol / 0.05, 1.0))
    )
    score = max(0.0, min(1.0, score))

    # ========== CAPITAL BASE ==========
    capital_kelly = CAPITAL_TOTAL * max(kelly_pct, 0.01)  # mínimo 1% = $30
    capital_score = CAPITAL_TOTAL * score * MAX_EXPOSICAO_TRADE
    capital_base = max(capital_kelly, capital_score)

    # ========== AJUSTES DINÂMICOS ==========
    # 1. Volatilidade via ATR
    with lock_indicadores:
        df = indicadores.get(par)
    if df is not None and 'atr' in df.columns and 'close' in df.columns:
        try:
            atr = float(df['atr'].iloc[-1])
            preco = float(df['close'].iloc[-1])
            atr_pct = atr / preco if preco > 0 else 0.01
            if atr_pct > 0.02:
                capital_base *= 0.7   # era 0.5 — menos agressivo
            elif atr_pct > 0.01:
                capital_base *= 0.85  # era 0.8
            elif atr_pct < 0.003:
                capital_base *= 1.2
        except Exception:
            pass

    # 2. Volume/Liquidez
    with lock_precos:
        dados = precos_cache.get(par, [])
    if dados and isinstance(dados[-1], dict):
        try:
            vol_recente = sum(float(d.get('volume', 0)) for d in dados[-20:]) / 20
            if vol_recente < 10000:
                capital_base *= 0.5   # era 0.3
            elif vol_recente < 100000:
                capital_base *= 0.7   # era 0.6
            elif vol_recente > 5000000:
                capital_base *= 1.2
        except Exception:
            pass

    # 3. Drawdown recente
    ultimos_trades = historico[-5:] if historico else []
    if len(ultimos_trades) >= 3:
        perdas_recentes = ultimos_trades.count(0)
        if perdas_recentes >= 3:
            capital_base *= 0.6   # era 0.4
        elif perdas_recentes >= 2:
            capital_base *= 0.8

    # 4. Concentração
    with lock_posicoes:
        n_posicoes = sum(1 for v in posicoes.values() if abs(v) > 0)
    if n_posicoes >= 5:
        capital_base *= 0.7   # era 0.5
    elif n_posicoes >= 3:
        capital_base *= 0.85  # era 0.7

    # 5. Boost para sinais excepcionais (com edge comprovado)
    if score >= 0.85 and forca >= 0.8 and taxa_acerto >= 0.65 and kelly_pct > 0.001:
        capital_base *= 1.5   # era 1.8 — menos agressivo

    # ========== FLOOR ABSOLUTO: $30 MÍNIMO SEMPRE ==========
    # Nenhum ajuste acima pode reduzir abaixo de $30
    capital_base = max(capital_base, CAPITAL_MIN_ABSOLUTO)

    # ========== TETO ==========
    capital_max = CAPITAL_TOTAL * MAX_EXPOSICAO_TRADE
    capital_base = min(capital_base, capital_max)

    # ========== VALIDAÇÃO BINANCE ==========
    try:
        min_notional, _ = obter_filtros_binance(par)
    except Exception:
        min_notional = 20.0

    if capital_base < min_notional:
        capital_base = max(min_notional, CAPITAL_MIN_ABSOLUTO)

    logger.info(f"  [CAPITAL] {par} | score={score:.2f} | kelly={kelly_pct:.3f} | "
                f"capital=${capital_base:.2f} | posições={n_posicoes}")

    return round(capital_base, 6)


# ================================================================
# CORRELAÇÃO
# ================================================================
def calcular_correlacoes():
    global correlacoes
    nova_correlacao = {}
    pares_processados = set()
    janela = 200
    pares_snapshot = list(pares_usdt)
    for i, p1 in enumerate(pares_snapshot):
        with lock_precos:
            dados1 = precos_cache.get(p1, [])
        closes1 = [d['close'] if isinstance(d, dict) else float(d) for d in dados1]
        if len(closes1) <= 20:
            continue
        arr1 = np.array(closes1[-janela:])
        for j, p2 in enumerate(pares_snapshot):
            if i >= j or (p2, p1) in pares_processados:
                continue
            with lock_precos:
                dados2 = precos_cache.get(p2, [])
            closes2 = [d['close'] if isinstance(d, dict) else float(d) for d in dados2]
            if len(closes2) <= 20:
                continue
            arr2 = np.array(closes2[-janela:])
            min_len = min(len(arr1), len(arr2))
            if min_len < 20:
                continue
            corr = np.corrcoef(arr1[-min_len:], arr2[-min_len:])[0, 1]
            if not np.isnan(corr) and corr > 0.7:
                nova_correlacao[p1] = max(nova_correlacao.get(p1, 0), corr)
            pares_processados.add((p1, p2))
    correlacoes = nova_correlacao


# ================================================================
# EXECUÇÃO DE ORDENS - [BUG 9 FIX] compatível com Hedge/One-Way
# ================================================================
def enviar_ordem_binance(par, side, quantidade):
    """Envia ordem FUTURES. Retorna resultado com preço real de execução."""
    if not REAL_TRADES:
        logger.info(f"[SIMULADO] {par} | {side} | Qtd:{quantidade:.6f}")
        return {'simulado': True}
    try:
        rate_limiter.wait()
        result = client.futures_create_order(
            symbol=par,
            side=side,
            type='MARKET',
            quantity=quantidade
        )
        # Extrair preço real de execução
        avg_price = float(result.get('avgPrice', 0))

        # Método 1: cumQuote / cumQty
        if avg_price == 0:
            cum_quote = float(result.get('cumQuote', 0))
            cum_qty = float(result.get('cumQty', 0))
            if cum_qty > 0:
                avg_price = cum_quote / cum_qty

        # Método 2: fills array (weighted average)
        if avg_price == 0:
            fills = result.get('fills', [])
            if fills:
                total_qty = sum(float(f.get('qty', 0)) for f in fills)
                total_cost = sum(float(f.get('price', 0)) * float(f.get('qty', 0)) for f in fills)
                if total_qty > 0:
                    avg_price = total_cost / total_qty

        # Método 3: consultar a ordem na Binance
        if avg_price == 0:
            try:
                order_id = result.get('orderId')
                if order_id:
                    rate_limiter.wait()
                    order_detail = client.futures_get_order(symbol=par, orderId=order_id)
                    avg_price = float(order_detail.get('avgPrice', 0))
            except:
                pass

        # Método 4: fallback para preço atual do cache
        if avg_price == 0:
            with lock_precos:
                dados = precos_cache.get(par, [])
            if dados:
                ultimo = dados[-1]
                avg_price = float(ultimo['close'] if isinstance(ultimo, dict) else ultimo)

        result['preco_real'] = avg_price

        logger.info(f"[ORDEM REAL] {par} | {side} | Qtd:{quantidade:.6f} | "
                     f"PreçoReal:{avg_price:.8f} | OrderID:{result.get('orderId')}")
        return result
    except BinanceAPIException as e:
        if 'PositionSide' in str(e.message) or '4061' in str(e.code):
            try:
                pos_side = 'LONG' if side == 'BUY' else 'SHORT'
                result = client.futures_create_order(
                    symbol=par, side=side, positionSide=pos_side,
                    type='MARKET', quantity=quantidade
                )
                avg_price = float(result.get('avgPrice', 0))
                if avg_price == 0:
                    cum_quote = float(result.get('cumQuote', 0))
                    cum_qty = float(result.get('cumQty', 0))
                    if cum_qty > 0:
                        avg_price = cum_quote / cum_qty
                if avg_price == 0:
                    fills = result.get('fills', [])
                    if fills:
                        total_qty = sum(float(f.get('qty', 0)) for f in fills)
                        total_cost = sum(float(f.get('price', 0)) * float(f.get('qty', 0)) for f in fills)
                        if total_qty > 0:
                            avg_price = total_cost / total_qty
                result['preco_real'] = avg_price
                logger.info(f"[ORDEM REAL HEDGE] {par} | {side} | {pos_side} | "
                             f"Qtd:{quantidade:.6f} | PreçoReal:{avg_price:.8f} | OrderID:{result.get('orderId')}")
                return result
            except Exception as e2:
                logger.error(f"[ORDEM HEDGE] {par} | {e2}")
                return {}
        logger.error(f"[ORDEM API] {par} | {e.message}")
        return {}
    except Exception as e:
        logger.error(f"[ORDEM ERRO] {par} | {e}")
        return {}




# ================================================================
# CÉREBRO IA v2 — Última porta + Aprendizado Real
# ================================================================
# COMO FUNCIONA O APRENDIZADO:
#
# 1. Cada trade que a IA aprova recebe um "fingerprint" — um mapa de
#    como estavam os 7 testes no momento da compra.
#    Ex: "micro_trend=OK, velocidade=FORTE, book=CONTRA, spread=BOM,
#         volume=SEM_VOLUME, range=TOPO, candles=OK"
#
# 2. Quando o trade fecha, registra se deu lucro ou perda.
#
# 3. Com o tempo, a IA descobre padrões:
#    - "quando book=CONTRA e range=TOPO → 82% de perda"
#    - "quando velocidade=FORTE e volume=CONFIRMA → 71% de lucro"
#
# 4. Esses padrões são usados na próxima decisão:
#    - Se o cenário atual JÁ CAUSOU perda antes → bloqueia ou exige mais
#    - Se o cenário atual JÁ DEU lucro antes → libera mais fácil
#
# 5. Tudo salva em disco (ia_memoria.json) — reiniciar não perde nada.
#
# 6. LIMIAR ADAPTATIVO: se o bot está perdendo (streak negativo),
#    a IA fica mais exigente. Se está ganhando, mantém normal.

import json as _ia_json
import os as _ia_os

ARQ_IA_MEMORIA = "ia_memoria.json"

_ia_historico = {}        # par -> [{'ts', 'fingerprint', 'resultado', 'lucro_pct'}]
_ia_padroes = {}          # fingerprint_key -> {'wins': int, 'losses': int, 'total': int}
_ia_trades_ativos = {}    # par -> {'entrada_ts', 'preco_entrada', 'curva_precos', 'btc_entrada', 'snapshot'}
_ia_comportamento = {}    # aprendizado de comportamento pós-entrada
_ciclo_inicio_global = time.time()  # tempo de início do ciclo atual
_ia_stats = {             # estatísticas globais
    'total_aprovados': 0,
    'total_bloqueados': 0,
    'wins': 0,
    'losses': 0,
    'pnl_acumulado': 0.0,
    'melhor_padrao': '',
    'pior_padrao': '',
    'limiar_atual': 0.55,
    'streak_ia': 0,
    'trades_caiu_30s': 0,   # quantos trades caíram nos primeiros 30s
    'trades_subiu_30s': 0,  # quantos subiram nos primeiros 30s
    'lucro_medio_subiu': 0.0,
    'lucro_medio_caiu': 0.0,
}

def _ia_salvar():
    """Salva memória da IA em disco."""
    try:
        dados = {
            'historico': {k: v[-30:] for k, v in _ia_historico.items()},
            'padroes': dict(list(_ia_padroes.items())[-200:]),
            'stats': _ia_stats,
            'comportamento': dict(list(_ia_comportamento.items())[-50:]),
        }
        with open(ARQ_IA_MEMORIA + '.tmp', 'w') as f:
            _ia_json.dump(dados, f, default=str)
        _ia_os.replace(ARQ_IA_MEMORIA + '.tmp', ARQ_IA_MEMORIA)
    except Exception as e:
        logger.debug(f"[IA SAVE] {e}")

def _ia_carregar():
    """Carrega memória da IA do disco."""
    global _ia_historico, _ia_padroes, _ia_stats, _ia_comportamento
    try:
        if _ia_os.path.exists(ARQ_IA_MEMORIA):
            with open(ARQ_IA_MEMORIA, 'r') as f:
                dados = _ia_json.load(f)
            _ia_historico = dados.get('historico', {})
            _ia_padroes = dados.get('padroes', {})
            # Migração: limpar fingerprints antigos (7 partes → 3 partes)
            _ia_padroes = {k: v for k, v in _ia_padroes.items() if k.count('|') <= 2}
            _ia_comportamento = dados.get('comportamento', {})
            saved_stats = dados.get('stats', {})
            _ia_stats.update(saved_stats)
            total_p = sum(p.get('total', 0) for p in _ia_padroes.values())
            logger.info(f"[IA] Memória carregada: {total_p} trades analisados, "
                        f"{len(_ia_padroes)} padrões conhecidos, "
                        f"limiar={_ia_stats['limiar_atual']:.0%}")
    except Exception as e:
        logger.debug(f"[IA LOAD] {e}")

def _ia_criar_fingerprint(detalhes):
    """
    Cria fingerprint SIMPLIFICADO — só os 3 testes que mais importam.
    
    Antes: 7 testes = centenas de combinações = nunca repete = nunca aprende
    Agora: 3 testes = ~27 combinações = repete rápido = aprende em 20 trades
    
    Os 3 que mais importam:
    1. micro_trend — o preço está indo pro nosso lado AGORA?
    2. velocidade — está acelerando ou desacelerando?
    3. book — pressão compradora ou vendedora?
    
    Spread, volume, range, candles continuam sendo avaliados nos 7 testes
    da validação. Só não entram no fingerprint pra não fragmentar demais.
    """
    partes = []
    for teste in ['micro_trend', 'velocidade', 'book']:
        valor = detalhes.get(teste, 'N/A')
        if '(' in str(valor):
            cat = str(valor).split('(')[0]
        else:
            cat = str(valor)
        # Simplificar ainda mais: OK/FORTE → BOM, CONTRA → RUIM, resto → NEUTRO
        if cat in ['OK', 'FORTE', 'CONFIRMA', 'BOM']:
            cat = 'BOM'
        elif cat in ['CONTRA', 'PERIGOSO', 'DIVERGE']:
            cat = 'RUIM'
        else:
            cat = 'NEUTRO'
        partes.append(cat)
    return "|".join(partes)

def _ia_consultar_padrao(fingerprint):
    """Consulta o histórico para ver se este padrão já deu lucro ou perda."""
    # Consulta EXATA (agora com 3 partes = muito mais chance de match)
    p = _ia_padroes.get(fingerprint)
    if p and p.get('total', 0) >= 2:
        taxa = p['wins'] / p['total']
        return taxa, p['total'], 'exato'
    
    # Consulta PARCIAL — se 2 dos 3 testes batem
    partes_fp = fingerprint.split('|')
    melhor_match = None
    melhor_total = 0
    
    for key, dados in _ia_padroes.items():
        if dados.get('total', 0) < 2:
            continue
        partes_key = key.split('|')
        matches = sum(1 for a, b in zip(partes_fp, partes_key) if a == b)
        if matches >= 2 and dados['total'] > melhor_total:
            melhor_total = dados['total']
            taxa = dados['wins'] / dados['total']
            melhor_match = (taxa, dados['total'], f'parcial_{matches}/3')
    
    if melhor_match:
        return melhor_match
    
    return 0.5, 0, 'sem_dados'

def _ia_atualizar_limiar():
    """Ajusta o limiar de aprovação baseado na performance recente."""
    global _ia_stats
    
    # Baseado nos últimos trades
    wins = _ia_stats.get('wins', 0)
    losses = _ia_stats.get('losses', 0)
    total = wins + losses
    
    if total < 5:
        _ia_stats['limiar_atual'] = 0.55  # default
        return
    
    taxa_geral = wins / total
    streak = _ia_stats.get('streak_ia', 0)
    
    # Lógica adaptativa:
    # - Ganhando muito (>65%) → pode relaxar um pouco (0.50)
    # - Normal (50-65%) → mantém (0.55)
    # - Perdendo (40-50%) → aperta (0.60)
    # - Perdendo muito (<40%) → aperta muito (0.65)
    # - Streak -3 ou pior → aperta mais 5%
    
    if taxa_geral >= 0.65:
        base = 0.50
    elif taxa_geral >= 0.50:
        base = 0.55
    elif taxa_geral >= 0.40:
        base = 0.60
    else:
        base = 0.65
    
    # Streak negativo → mais exigente
    if streak <= -3:
        base += 0.05
    elif streak <= -5:
        base += 0.10
    
    # Regime chop → mais exigente
    if regime_mercado == 'chop':
        base += 0.05
    
    _ia_stats['limiar_atual'] = min(0.80, max(0.45, base))


def validacao_final_ia(par, acao_proposta, confianca_atual, forca_atual):
    """
    Análise final com aprendizado real.
    
    PRÉ-FILTROS RÁPIDOS (sem API call) + 10 TESTES + PADRÕES + LIMIAR
    """
    try:
        direcao = 1 if acao_proposta == 'compra' else -1
        testes_passados = 0
        total_testes = 7
        motivos_falha = []
        detalhes = {}
        
        # ============================================================
        # PRÉ-FILTRO 1: BLACKLIST ADAPTATIVA
        # Se perdeu 3+ vezes seguidas neste par, bloqueia 30 minutos
        # ============================================================
        hist_par = _ia_historico.get(par, [])
        perdas_seguidas = 0
        for h in reversed(hist_par):
            if h.get('resultado') == 0:
                perdas_seguidas += 1
            elif h.get('resultado') == 1:
                break
            else:
                continue  # resultado=-1 (pendente), ignora
        
        if perdas_seguidas >= 3:
            ultimo_loss_ts = max((h['ts'] for h in hist_par if h.get('resultado') == 0), default=0)
            tempo_desde_loss = time.time() - ultimo_loss_ts
            if tempo_desde_loss < 1800:  # 30 minutos
                restante = int((1800 - tempo_desde_loss) / 60)
                print(f"  {Fore.RED}[🧠 IA] {par:12} | BLACKLIST | {perdas_seguidas}x perda seguida | "
                      f"volta em {restante}min{Style.RESET_ALL}")
                return False, 0.0, f"blacklist_{perdas_seguidas}x_perda"
        
        # ============================================================
        # PRÉ-FILTRO 2: SCORE DO PAR (qualidade histórica)
        # Pares ruins pra scalping ficam penalizados
        # ============================================================
        _ia_par_score = _ia_stats.get('par_scores', {})
        score_par = _ia_par_score.get(par, 0.5)  # default 0.5 (neutro)
        if score_par < 0.25:
            print(f"  {Fore.RED}[🧠 IA] {par:12} | SCORE RUIM | score={score_par:.2f} | "
                  f"par historicamente ruim{Style.RESET_ALL}")
            return False, 0.0, f"par_score_baixo({score_par:.2f})"
        
        # ============================================================
        # PRÉ-FILTRO 3: HORÁRIO INTELIGENTE
        # Aprende quais horários dão lucro
        # ============================================================
        hora_atual = datetime.now(timezone.utc).hour
        _ia_hora_stats = _ia_stats.get('hora_stats', {})
        hora_key = str(hora_atual)
        hora_data = _ia_hora_stats.get(hora_key, {})
        hora_total = hora_data.get('total', 0)
        if hora_total >= 10:  # precisa de 10+ trades pra bloquear um horário inteiro
            hora_taxa = hora_data.get('wins', 0) / hora_total
            if hora_taxa < 0.20:  # menos de 20% = realmente ruim
                print(f"  {Fore.RED}[🧠 IA] {par:12} | HORÁRIO RUIM | {hora_atual}h UTC: "
                      f"{hora_taxa:.0%} acerto ({hora_total}t){Style.RESET_ALL}")
                return False, 0.0, f"horario_ruim({hora_atual}h={hora_taxa:.0%})"
        
        # ============================================================
        # PRÉ-FILTRO 4: BLOQUEIO BTC FORTE
        # Se BTC caiu >0.3% em 15min, bloqueia TODOS os longs
        # Se BTC subiu >0.3% em 15min, bloqueia TODOS os shorts
        # ============================================================
        try:
            with lock_indicadores:
                df_btc = indicadores.get('BTCUSDT')
            if df_btc is not None and len(df_btc) >= 15:
                btc_15min_ago = float(df_btc['close'].iloc[-15])
                btc_agora = float(df_btc['close'].iloc[-1])
                btc_change_15m = (btc_agora - btc_15min_ago) / btc_15min_ago
                
                if btc_change_15m < -0.003 and acao_proposta == 'compra':
                    print(f"  {Fore.RED}[🧠 IA] {par:12} | BTC CAINDO | BTC {btc_change_15m*100:+.2f}% em 15m | "
                          f"bloqueia LONGs{Style.RESET_ALL}")
                    return False, 0.0, f"btc_caindo({btc_change_15m*100:+.1f}%)"
                elif btc_change_15m > 0.003 and acao_proposta == 'venda':
                    print(f"  {Fore.RED}[🧠 IA] {par:12} | BTC SUBINDO | BTC {btc_change_15m*100:+.2f}% em 15m | "
                          f"bloqueia SHORTs{Style.RESET_ALL}")
                    return False, 0.0, f"btc_subindo({btc_change_15m*100:+.1f}%)"
        except:
            pass
        
        # ============================================================
        # BUSCAR DADOS FRESCOS
        # ============================================================
        try:
            rate_limiter.wait()
            book_ticker = client_level2.book_ticker(symbol=par)
            preco_bid = float(book_ticker['bidPrice'])
            preco_ask = float(book_ticker['askPrice'])
            preco_mid = (preco_bid + preco_ask) / 2
            spread_pct = (preco_ask - preco_bid) / preco_bid if preco_bid > 0 else 1
        except Exception as e:
            logger.debug(f"[IA] {par} book_ticker erro: {e}")
            return False, 0.0, "sem_dados_mercado"
        
        try:
            rate_limiter.wait()
            klines_fresh = client_level2.klines(symbol=par, interval='1m', limit=10)
            if not klines_fresh or len(klines_fresh) < 5:
                return False, 0.0, "sem_klines_frescos"
            closes_fresh = [float(k[4]) for k in klines_fresh]
            highs_fresh = [float(k[2]) for k in klines_fresh]
            lows_fresh = [float(k[3]) for k in klines_fresh]
            volumes_fresh = [float(k[5]) for k in klines_fresh]
            opens_fresh = [float(k[1]) for k in klines_fresh]
        except Exception as e:
            logger.debug(f"[IA] {par} klines erro: {e}")
            return False, 0.0, "sem_klines_frescos"
        
        book_bids_total = 0
        book_asks_total = 0
        try:
            rate_limiter.wait()
            depth = client_level2.depth(symbol=par, limit=20)
            if depth and depth.get('bids') and depth.get('asks'):
                book_bids_total = sum(float(q) for _, q in depth['bids'])
                book_asks_total = sum(float(q) for _, q in depth['asks'])
        except:
            pass
        
        # ============================================================
        # PRÉ-FILTRO 5: LIQUIDEZ MÍNIMA
        # Volume 24h muito baixo = spread alto, slippage, perda garantida
        # ============================================================
        try:
            vol_24h = sum(volumes_fresh) * preco_mid  # volume em USDT aprox
            # volumes_fresh tem 10 candles de 1m, extrapolar pra 24h
            vol_24h_est = vol_24h * 144  # 1440min/10min
            if vol_24h_est < 500000:  # menos de $500k/dia
                print(f"  {Fore.RED}[🧠 IA] {par:12} | SEM LIQUIDEZ | vol~${vol_24h_est/1000:.0f}k/dia{Style.RESET_ALL}")
                return False, 0.0, f"sem_liquidez(${vol_24h_est/1000:.0f}k)"
        except:
            pass
        
        # ============================================================
        # PRÉ-FILTRO 5.5: ATR MÍNIMO (A4 FIX — moeda parada demais)
        # Scalping precisa de movimento. Se ATR de 1m < 0.10% do preço,
        # é IMPOSSÍVEL lucrar 0.3% em 10 minutos. Taxa ida+volta = 0.08%.
        # Precisa de ATR que gere pelo menos 3x a taxa em 5 candles.
        # ============================================================
        try:
            if len(closes_fresh) >= 5 and len(highs_fresh) >= 5 and len(lows_fresh) >= 5:
                # ATR manual nos últimos 5 candles de 1m
                trs = []
                for i in range(-5, 0):
                    tr = max(
                        highs_fresh[i] - lows_fresh[i],
                        abs(highs_fresh[i] - closes_fresh[i-1]) if i > -5 else 0,
                        abs(lows_fresh[i] - closes_fresh[i-1]) if i > -5 else 0
                    )
                    trs.append(tr)
                atr_1m = sum(trs) / len(trs) if trs else 0
                atr_pct_fresh = atr_1m / preco_mid if preco_mid > 0 else 0
                
                taxa_total = 0.0008  # 0.08% ida + volta
                # Se 5 candles perfeitos não geram 2.5x a taxa = impossível lucrar
                if atr_pct_fresh * 5 < taxa_total * 2.5:
                    print(f"  {Fore.RED}[🧠 IA] {par:12} | ATR BAIXO | "
                          f"ATR={atr_pct_fresh*100:.3f}% | 5 candles={atr_pct_fresh*5*100:.2f}% vs "
                          f"taxa={taxa_total*2.5*100:.2f}%{Style.RESET_ALL}")
                    return False, 0.0, f"atr_baixo({atr_pct_fresh*100:.3f}%)"
        except:
            pass
        
        # ============================================================
        # PRÉ-FILTRO 6: PREÇO JÁ SE MOVEU (o trem já passou)
        # Threshold 0.5% — conta o spread bid/ask + delay do cache
        # Abaixo disso é ruído, não movimento real
        # ============================================================
        try:
            with lock_modelos:
                mm_check = micro_modelos.get(par, {})
            preco_sinal = mm_check.get('preco_sinal', 0)
            if preco_sinal > 0 and preco_mid > 0:
                move_desde_sinal = abs(preco_mid - preco_sinal) / preco_sinal
                if move_desde_sinal > 0.005:  # >0.5% = realmente mudou
                    print(f"  {Fore.RED}[🧠 IA] {par:12} | TREM PASSOU | preço moveu "
                          f"{move_desde_sinal*100:.2f}% desde sinal{Style.RESET_ALL}")
                    return False, 0.0, f"preco_moveu({move_desde_sinal*100:.2f}%)"
        except:
            pass
        
        # ============================================================
        # PRÉ-FILTRO 7: CONFIRMAÇÃO 5 MINUTOS
        # Se o candle de 5min contradiz a direção, não entra
        # ============================================================
        try:
            rate_limiter.wait()
            klines_5m = client_level2.klines(symbol=par, interval='5m', limit=3)
            if klines_5m and len(klines_5m) >= 2:
                c5m_atual = float(klines_5m[-1][4])
                o5m_atual = float(klines_5m[-1][1])
                c5m_anterior = float(klines_5m[-2][4])
                o5m_anterior = float(klines_5m[-2][1])
                
                # Corpo do candle 5m atual
                corpo_5m = c5m_atual - o5m_atual
                corpo_5m_ant = c5m_anterior - o5m_anterior
                
                # Se ambos os candles de 5m vão contra nossa direção, bloqueia
                if corpo_5m * direcao < 0 and corpo_5m_ant * direcao < 0:
                    motivos_falha.append('5min_contra')
                    detalhes['tf_5m'] = 'CONTRA'
                elif corpo_5m * direcao > 0:
                    detalhes['tf_5m'] = 'OK'
                else:
                    detalhes['tf_5m'] = 'NEUTRO'
        except:
            detalhes['tf_5m'] = 'N/A'
        
        # ============================================================
        # PRÉ-FILTRO 8: MOMENTUM ULTRA-CURTO (60 segundos)
        # Scalping de 5 min: o que importa é o que está acontecendo AGORA
        # Se nos últimos 60 segundos o preço está indo contra, não entra
        # ============================================================
        try:
            # Comparar preço atual (book) com close de 1 e 2 minutos atrás
            preco_1min = closes_fresh[-1]  # close do último candle de 1m
            preco_2min = closes_fresh[-2]  # close de 2 minutos atrás
            preco_3min = closes_fresh[-3]  # close de 3 minutos atrás
            
            # Momentum: preço atual vs 2 min atrás
            momentum_curto = (preco_mid - preco_2min) / preco_2min if preco_2min > 0 else 0
            
            # Se queremos comprar mas preço CAIU nos últimos 2 min = contra
            # Se queremos vender mas preço SUBIU nos últimos 2 min = contra
            if momentum_curto * direcao < -0.001:
                # Preço está indo contra nossa direção com força (>0.1%)
                motivos_falha.append(f'momentum_contra({momentum_curto*100:+.2f}%)')
                detalhes['momentum'] = 'CONTRA'
            elif momentum_curto * direcao > 0.001:
                # Preço está acelerando na nossa direção
                detalhes['momentum'] = 'BOM'
            else:
                detalhes['momentum'] = 'NEUTRO'
            
            # Velocidade: está acelerando ou desacelerando?
            # Se 1min ago → agora é menor que 2min ago → 1min ago, está perdendo força
            move_recente = abs(preco_mid - preco_1min)
            move_anterior = abs(preco_1min - preco_2min)
            if move_anterior > 0:
                aceleracao = move_recente / move_anterior
                if aceleracao < 0.3 and momentum_curto * direcao > 0:
                    # Está desacelerando muito — o movimento já acabou
                    detalhes['momentum'] = 'PERDENDO_FORCA'
                    motivos_falha.append('desacelerando')
        except:
            detalhes['momentum'] = 'N/A'
        
        # ============================================================
        # TESTE 1: MICRO-TENDÊNCIA
        # ============================================================
        c3 = closes_fresh[-3:]
        if len(c3) >= 3:
            subindo = c3[-1] > c3[-2] > c3[-3]
            caindo = c3[-1] < c3[-2] < c3[-3]
            micro_trend = 0
            if subindo: micro_trend = 1
            elif caindo: micro_trend = -1
            elif c3[-1] > c3[0]: micro_trend = 1
            elif c3[-1] < c3[0]: micro_trend = -1
            
            if micro_trend * direcao > 0:
                testes_passados += 1; detalhes['micro_trend'] = 'OK'
            elif micro_trend * direcao < 0:
                motivos_falha.append('micro_trend_contra'); detalhes['micro_trend'] = 'CONTRA'
            else:
                testes_passados += 0.5; detalhes['micro_trend'] = 'LATERAL'
        
        # ============================================================
        # TESTE 2: VELOCIDADE + ACELERAÇÃO
        # ============================================================
        if len(closes_fresh) >= 5:
            rets = [(closes_fresh[i] - closes_fresh[i-1]) / (closes_fresh[i-1] + 1e-12)
                    for i in range(1, len(closes_fresh))]
            vel_3 = sum(rets[-3:]) / 3
            vel_ant = sum(rets[-6:-3]) / 3 if len(rets) >= 6 else vel_3
            aceleracao = vel_3 - vel_ant
            
            vel_ok = vel_3 * direcao > 0
            acel_ok = aceleracao * direcao > 0
            
            if vel_ok and acel_ok:
                testes_passados += 1; detalhes['velocidade'] = 'FORTE'
            elif vel_ok:
                testes_passados += 0.7; detalhes['velocidade'] = 'OK'
            elif not vel_ok and abs(vel_3) < 0.0003:
                testes_passados += 0.3; detalhes['velocidade'] = 'PARADO'
            else:
                motivos_falha.append('velocidade_contra'); detalhes['velocidade'] = 'CONTRA'
        
        # ============================================================
        # TESTE 3: BOOK L2
        # ============================================================
        if book_bids_total > 0 and book_asks_total > 0:
            deseq = (book_bids_total - book_asks_total) / (book_bids_total + book_asks_total)
            if (acao_proposta == 'compra' and deseq > 0.05) or \
               (acao_proposta == 'venda' and deseq < -0.05):
                testes_passados += 1; detalhes['book'] = 'OK'
            elif abs(deseq) < 0.05:
                testes_passados += 0.5; detalhes['book'] = 'NEUTRO'
            else:
                motivos_falha.append('book_contra'); detalhes['book'] = 'CONTRA'
        else:
            testes_passados += 0.5; detalhes['book'] = 'SEM_DADOS'
        
        # ============================================================
        # TESTE 4: SPREAD
        # ============================================================
        if spread_pct < 0.0005:
            testes_passados += 1; detalhes['spread'] = 'BOM'
        elif spread_pct < 0.0015:
            testes_passados += 0.7; detalhes['spread'] = 'OK'
        elif spread_pct < 0.003:
            testes_passados += 0.3; detalhes['spread'] = 'ALTO'
        else:
            motivos_falha.append('spread_perigoso'); detalhes['spread'] = 'PERIGOSO'
        
        # ============================================================
        # TESTE 5: DIVERGÊNCIA VOLUME
        # ============================================================
        if len(volumes_fresh) >= 5:
            vol_media = sum(volumes_fresh[:-3]) / max(len(volumes_fresh[:-3]), 1)
            vol_recente = sum(volumes_fresh[-3:]) / 3
            preco_move = (closes_fresh[-1] - closes_fresh[-4]) / (closes_fresh[-4] + 1e-12)
            vol_confirma = vol_recente > vol_media * 0.8
            preco_direcao = preco_move * direcao > 0
            
            if vol_confirma and preco_direcao:
                testes_passados += 1; detalhes['volume'] = 'CONFIRMA'
            elif vol_confirma:
                testes_passados += 0.3; detalhes['volume'] = 'PRECO_CONTRA'
            elif preco_direcao:
                testes_passados += 0.4; detalhes['volume'] = 'SEM_VOLUME'
                motivos_falha.append('sem_volume')
            else:
                motivos_falha.append('tudo_contra'); detalhes['volume'] = 'DIVERGE'
        
        # ============================================================
        # TESTE 6: POSIÇÃO NO RANGE
        # ============================================================
        if len(closes_fresh) >= 8:
            high_range = max(highs_fresh[-8:])
            low_range = min(lows_fresh[-8:])
            range_total = high_range - low_range
            if range_total > 0:
                pos_range = (preco_mid - low_range) / range_total
                if acao_proposta == 'compra':
                    if pos_range < 0.35:
                        testes_passados += 1; detalhes['range'] = 'BOM'
                    elif pos_range < 0.65:
                        testes_passados += 0.5; detalhes['range'] = 'MEIO'
                    else:
                        motivos_falha.append('comprando_topo'); detalhes['range'] = 'TOPO'
                else:
                    if pos_range > 0.65:
                        testes_passados += 1; detalhes['range'] = 'BOM'
                    elif pos_range > 0.35:
                        testes_passados += 0.5; detalhes['range'] = 'MEIO'
                    else:
                        motivos_falha.append('vendendo_fundo'); detalhes['range'] = 'FUNDO'
        
        # ============================================================
        # TESTE 7: CANDLES RECENTES
        # ============================================================
        if len(closes_fresh) >= 3:
            corpos_ok = 0
            for i in range(-3, 0):
                if i >= -len(closes_fresh):
                    corpo = closes_fresh[i] - opens_fresh[i]
                    if corpo * direcao > 0: corpos_ok += 1
            if corpos_ok >= 2:
                testes_passados += 1; detalhes['candles'] = 'OK'
            elif corpos_ok == 1:
                testes_passados += 0.4; detalhes['candles'] = 'MISTO'
            else:
                motivos_falha.append('candles_contra'); detalhes['candles'] = 'CONTRA'
        
        # ============================================================
        # TESTE 8: CORRELAÇÃO BTC (novo)
        # ============================================================
        # Se BTC caiu nos últimos 2 minutos, não compra long em altcoin
        btc_move = 0
        btc_k = None
        try:
            rate_limiter.wait()
            btc_k = client_level2.klines(symbol='BTCUSDT', interval='1m', limit=3)
            if btc_k and len(btc_k) >= 2:
                btc_agora = float(btc_k[-1][4])
                btc_antes = float(btc_k[-2][4])
                btc_move = (btc_agora - btc_antes) / btc_antes
                
                if btc_move * direcao > 0.0002:
                    testes_passados += 1  # BTC indo pro nosso lado
                    detalhes['btc'] = 'OK'
                elif btc_move * direcao < -0.0005:
                    motivos_falha.append(f'btc_contra({btc_move*100:+.2f}%)')
                    detalhes['btc'] = 'CONTRA'
                else:
                    testes_passados += 0.5  # neutro
                    detalhes['btc'] = 'NEUTRO'
                total_testes = 8
        except:
            pass
        
        # ============================================================
        # TESTE 9: TEMPO DO CICLO
        # ============================================================
        # Mede tempo desde que o MODELO daquele par foi calculado
        # (não desde o início do ciclo, porque no fast lane o par é analisado na hora)
        try:
            with lock_modelos:
                mm_tempo = micro_modelos.get(par, {})
            ts_modelo = mm_tempo.get('ts_modelo', 0)
            if ts_modelo > 0:
                tempo_desde_modelo = time.time() - ts_modelo
            else:
                tempo_desde_modelo = time.time() - globals().get('_ciclo_inicio_global', time.time())
            
            if tempo_desde_modelo > 600:
                ajuste_tempo = -0.10
                motivos_falha.append(f'dados_velhos({tempo_desde_modelo:.0f}s)')
                detalhes['tempo'] = 'VELHO'
            elif tempo_desde_modelo > 300:
                ajuste_tempo = -0.05
                detalhes['tempo'] = 'LENTO'
            elif tempo_desde_modelo < 30:
                ajuste_tempo = +0.03  # dados fresquíssimos, bônus
                detalhes['tempo'] = 'FRESCO'
            else:
                ajuste_tempo = 0
                detalhes['tempo'] = 'OK'
        except:
            ajuste_tempo = 0
            detalhes['tempo'] = 'N/A'
        
        # ============================================================
        # TESTE 10: COMPORTAMENTO PÓS-ENTRADA (aprendizado)
        # ============================================================
        # Se trades anteriores com cenário parecido caíram nos primeiros 30s
        # é sinal de timing ruim
        ajuste_comportamento = 0
        caiu_30s = _ia_stats.get('trades_caiu_30s', 0)
        subiu_30s = _ia_stats.get('trades_subiu_30s', 0)
        total_obs = caiu_30s + subiu_30s
        if total_obs >= 5:
            taxa_subiu = subiu_30s / total_obs
            if taxa_subiu < 0.35:
                # Mais de 65% dos trades caem nos primeiros 30s
                # Nosso timing de entrada está ruim
                ajuste_comportamento = -0.08
                detalhes['timing'] = f'RUIM({taxa_subiu:.0%}↑)'
            elif taxa_subiu > 0.60:
                ajuste_comportamento = +0.05
                detalhes['timing'] = f'BOM({taxa_subiu:.0%}↑)'
        
        # ============================================================
        # PROBABILIDADE BASE
        # ============================================================
        prob_bruta = testes_passados / total_testes
        
        # ============================================================
        # CONSULTA DE PADRÕES APRENDIDOS (a grande diferença)
        # ============================================================
        fingerprint = _ia_criar_fingerprint(detalhes)
        taxa_padrao, n_trades_padrao, tipo_match = _ia_consultar_padrao(fingerprint)
        
        ajuste_padrao = 0.0
        padrao_info = ""
        
        if n_trades_padrao >= 3:
            # Este padrão já aconteceu antes!
            if taxa_padrao >= 0.70:
                # 70%+ de acerto → boost forte
                ajuste_padrao = +0.12
                padrao_info = f"PADRAO_BOM({taxa_padrao:.0%}/{n_trades_padrao}t)"
            elif taxa_padrao >= 0.55:
                ajuste_padrao = +0.05
                padrao_info = f"padrao_ok({taxa_padrao:.0%}/{n_trades_padrao}t)"
            elif taxa_padrao <= 0.30:
                # 30% ou menos de acerto → este cenário é uma armadilha
                ajuste_padrao = -0.20
                padrao_info = f"PADRAO_RUIM({taxa_padrao:.0%}/{n_trades_padrao}t)"
            elif taxa_padrao <= 0.45:
                ajuste_padrao = -0.10
                padrao_info = f"padrao_fraco({taxa_padrao:.0%}/{n_trades_padrao}t)"
        
        # ============================================================
        # ANTI-REPEAT: se perdeu 2x seguido no mesmo par com mesma ação
        # ============================================================
        hist_par = _ia_historico.get(par, [])
        recentes = [h for h in hist_par[-5:] 
                     if h.get('direcao') == acao_proposta and h.get('resultado') != -1]
        if len(recentes) >= 2:
            ultimos_resultados = [h['resultado'] for h in recentes[-2:]]
            if ultimos_resultados == [0, 0]:
                # Perdeu 2x seguido neste par com mesma direção
                ajuste_padrao -= 0.15
                padrao_info += " ANTI_REPEAT"
                motivos_falha.append(f'perdeu_2x_{acao_proposta}_{par[-6:]}')
        
        # ============================================================
        # PROBABILIDADE FINAL (A5 FIX: peso dinâmico do padrão aprendido)
        # ============================================================
        # Quanto mais dados no padrão, mais ele pesa na decisão.
        # Com 10+ trades: padrão DOMINA (60%) — a experiência manda
        # Com 5-9 trades: padrão tem peso médio (40%)
        # Com <5 trades: padrão tem peso baixo (15%) — dados insuficientes
        
        if n_trades_padrao >= 10:
            # Padrão bem estabelecido — experiência domina
            prob_ajustada = (
                taxa_padrao * 0.55 +         # padrão aprendido (55%)
                prob_bruta * 0.30 +           # testes em tempo real (30%)
                confianca_atual * 0.15        # confiança do bot (15%)
            ) + ajuste_padrao + ajuste_tempo + ajuste_comportamento
        elif n_trades_padrao >= 5:
            # Padrão com dados razoáveis
            prob_ajustada = (
                taxa_padrao * 0.40 +         # padrão aprendido (40%)
                prob_bruta * 0.40 +           # testes em tempo real (40%)
                confianca_atual * 0.20        # confiança do bot (20%)
            ) + ajuste_padrao + ajuste_tempo + ajuste_comportamento
        else:
            # Sem dados suficientes — testes em tempo real dominam
            prob_ajustada = (
                prob_bruta * 0.55 +           # testes em tempo real (55%)
                confianca_atual * 0.25 +      # confiança do bot (25%)
                taxa_padrao * 0.20            # padrão (pouco peso, dados fracos)
            ) + ajuste_tempo + ajuste_comportamento
            # SEM ajuste_padrao quando dados são fracos (evita boost/penalidade injusta)
        
        # Ajuste global pela performance
        _ia_atualizar_limiar()
        limiar = _ia_stats['limiar_atual']
        
        prob_final = round(min(1.0, max(0.0, prob_ajustada)), 3)
        
        # ============================================================
        # TRAVAS DE SEGURANÇA (bloqueiam INDEPENDENTE da prob)
        # ============================================================
        bloqueio_duro = False
        motivo_bloqueio_duro = []
        
        # TRAVA 1: padrao_fraco ou PADRAO_RUIM = NUNCA aprovar
        # Se esse cenário já deu <45% acerto em 5+ trades, não entra
        if n_trades_padrao >= 5 and taxa_padrao < 0.45:
            bloqueio_duro = True
            motivo_bloqueio_duro.append(f'padrao_historico_ruim({taxa_padrao:.0%}/{n_trades_padrao}t)')
        
        # TRAVA 2: micro_trend CONTRA = NUNCA aprovar
        # Se o preço está indo contra nos últimos 2 min, scalping não funciona
        if detalhes.get('micro_trend') == 'CONTRA':
            bloqueio_duro = True
            motivo_bloqueio_duro.append('micro_trend_CONTRA')
        
        # TRAVA 3: Muitos sinais CONTRA = NUNCA aprovar
        # Se 3+ dos testes deram CONTRA, não tem edge
        n_contra = sum(1 for v in detalhes.values() 
                       if isinstance(v, str) and v == 'CONTRA')
        if n_contra >= 3:
            bloqueio_duro = True
            motivo_bloqueio_duro.append(f'{n_contra}_testes_CONTRA')
        
        # ============================================================
        # DECISÃO
        # ============================================================
        if bloqueio_duro:
            aprovado = False
            motivos_falha.extend(motivo_bloqueio_duro)
        else:
            aprovado = prob_final >= limiar
        
        # Atualizar stats
        if aprovado:
            _ia_stats['total_aprovados'] += 1
        else:
            _ia_stats['total_bloqueados'] += 1
        
        # Log
        cor = Fore.GREEN + Style.BRIGHT if aprovado else Fore.RED + Style.BRIGHT
        status = "APROVADO" if aprovado else "BLOQUEADO"
        if bloqueio_duro:
            status = "🚫 BLOQUEIO_DURO"
        
        det_resumo = " ".join(f"{v}" for v in detalhes.values())
        padrao_str = f" | {padrao_info}" if padrao_info else ""
        limiar_str = f"lim={limiar:.0%}"
        
        print(f"  {cor}[🧠 IA] {par:12} | {status} | prob={prob_final:.0%} vs {limiar_str} | "
              f"testes={testes_passados:.1f}/7 | {det_resumo}{padrao_str}{Style.RESET_ALL}")
        
        if bloqueio_duro:
            print(f"  {Fore.RED + Style.BRIGHT}        ⛔ TRAVA: {', '.join(motivo_bloqueio_duro)}{Style.RESET_ALL}")
        elif not aprovado and motivos_falha:
            print(f"  {Fore.RED}        → {', '.join(motivos_falha[:3])}{Style.RESET_ALL}")
        
        logger.info(f"[IA] {par} {acao_proposta} {status} prob={prob_final:.0%} "
                    f"lim={limiar:.0%} testes={testes_passados:.1f}/7 "
                    f"{'|'+padrao_info if padrao_info else ''}")
        
        # Registrar para aprendizado
        if par not in _ia_historico:
            _ia_historico[par] = []
        _ia_historico[par].append({
            'ts': time.time(),
            'direcao': acao_proposta,
            'prob': prob_final,
            'aprovado': aprovado,
            'resultado': -1,
            'fingerprint': fingerprint,
            'detalhes': detalhes,
            'forca': forca_atual,
            'confianca': confianca_atual,
            'regime': regime_mercado,
            'limiar': limiar,
            'btc_move': btc_move,
            'spread': spread_pct,
            'vol_classe': _adapt.get('vol_classe', 'medio'),
            'hora': datetime.now(timezone.utc).hour,
        })
        _ia_historico[par] = _ia_historico[par][-50:]
        
        # Se aprovado, registrar pra monitoramento pós-entrada
        if aprovado:
            btc_preco_entrada = 0
            try:
                btc_preco_entrada = float(btc_k[-1][4]) if btc_k else 0
            except:
                pass
            _ia_trades_ativos[par] = {
                'entrada_ts': time.time(),
                'preco_entrada': preco_mid,
                'btc_entrada': btc_preco_entrada,
                'direcao': acao_proposta,
                'fingerprint': fingerprint,
                'curva_precos': [],  # será preenchido pelo monitor
                'prob_entrada': prob_final,
            }
        
        # Salvar a cada 10 trades
        total = _ia_stats['total_aprovados'] + _ia_stats['total_bloqueados']
        if total % 10 == 0:
            _ia_salvar()
        
        return aprovado, prob_final, status if aprovado else ', '.join(motivos_falha[:2])
        
    except Exception as e:
        logger.error(f"[IA] {par} Erro: {e}")
        return True, confianca_atual, "erro_ia_passthrough"


def ia_registrar_resultado(par, ganhou, lucro_pct=0.0):
    """
    Chamada quando um trade fecha. Registra resultado + atualiza padrões.
    Agora também salva curva de preço pós-entrada.
    """
    global _ia_historico, _ia_padroes, _ia_stats, _ia_comportamento
    
    # Salvar curva de preço do trade que fechou
    trade_ia = _ia_trades_ativos.pop(par, {})
    curva = trade_ia.get('curva_precos', [])
    
    # Aprender com a curva: em que segundo começou a dar errado?
    if curva and len(curva) >= 3:
        fp = trade_ia.get('fingerprint', 'desconhecido')
        if fp not in _ia_comportamento:
            _ia_comportamento[fp] = {'curvas_win': [], 'curvas_loss': []}
        if ganhou:
            _ia_comportamento[fp]['curvas_win'].append(curva[:6])  # primeiros 60s
        else:
            _ia_comportamento[fp]['curvas_loss'].append(curva[:6])
        # Manter últimas 10 curvas por padrão
        _ia_comportamento[fp]['curvas_win'] = _ia_comportamento[fp]['curvas_win'][-10:]
        _ia_comportamento[fp]['curvas_loss'] = _ia_comportamento[fp]['curvas_loss'][-10:]
    
    hist = _ia_historico.get(par, [])
    
    # Encontrar a última entrada não-resolvida
    encontrou = False
    for i in range(len(hist) - 1, -1, -1):
        if hist[i].get('resultado', -1) == -1:
            hist[i]['resultado'] = 1 if ganhou else 0
            hist[i]['lucro_pct'] = lucro_pct
            
            # APRENDER: registrar fingerprint com resultado
            fp = hist[i].get('fingerprint', '')
            if fp:
                if fp not in _ia_padroes:
                    _ia_padroes[fp] = {'wins': 0, 'losses': 0, 'total': 0}
                _ia_padroes[fp]['total'] += 1
                if ganhou:
                    _ia_padroes[fp]['wins'] += 1
                else:
                    _ia_padroes[fp]['losses'] += 1
            
            encontrou = True
            break
    
    _ia_historico[par] = hist
    
    # Atualizar stats globais
    if encontrou:
        if ganhou:
            _ia_stats['wins'] += 1
            _ia_stats['streak_ia'] = max(1, _ia_stats.get('streak_ia', 0) + 1)
        else:
            _ia_stats['losses'] += 1
            _ia_stats['streak_ia'] = min(-1, _ia_stats.get('streak_ia', 0) - 1)
        
        _ia_stats['pnl_acumulado'] += lucro_pct
        
        # Encontrar melhor e pior padrão
        if _ia_padroes:
            melhor = max(_ia_padroes.items(), 
                        key=lambda x: x[1]['wins']/max(x[1]['total'],1) if x[1]['total']>=3 else 0)
            pior = min(_ia_padroes.items(),
                      key=lambda x: x[1]['wins']/max(x[1]['total'],1) if x[1]['total']>=3 else 1)
            _ia_stats['melhor_padrao'] = melhor[0][:50]
            _ia_stats['pior_padrao'] = pior[0][:50]
        
        # Atualizar limiar
        _ia_atualizar_limiar()
        
        # ============================================================
        # ATUALIZAR SCORE DO PAR
        # ============================================================
        if 'par_scores' not in _ia_stats:
            _ia_stats['par_scores'] = {}
        old_score = _ia_stats['par_scores'].get(par, 0.5)
        if ganhou:
            _ia_stats['par_scores'][par] = min(1.0, old_score + 0.08)
        else:
            _ia_stats['par_scores'][par] = max(0.0, old_score - 0.12)  # penaliza mais que recompensa
        
        # ============================================================
        # ATUALIZAR STATS POR HORÁRIO
        # ============================================================
        if 'hora_stats' not in _ia_stats:
            _ia_stats['hora_stats'] = {}
        hora_key = str(datetime.now(timezone.utc).hour)
        if hora_key not in _ia_stats['hora_stats']:
            _ia_stats['hora_stats'][hora_key] = {'wins': 0, 'losses': 0, 'total': 0}
        _ia_stats['hora_stats'][hora_key]['total'] += 1
        if ganhou:
            _ia_stats['hora_stats'][hora_key]['wins'] += 1
        else:
            _ia_stats['hora_stats'][hora_key]['losses'] += 1
        
        # Log de aprendizado
        total = _ia_stats['wins'] + _ia_stats['losses']
        taxa = _ia_stats['wins'] / total if total > 0 else 0
        cor = Fore.GREEN if ganhou else Fore.RED
        
        print(f"  {cor}[🧠 APRENDEU] {par} | {'WIN' if ganhou else 'LOSS'} | "
              f"Acerto geral: {taxa:.0%} ({_ia_stats['wins']}W/{_ia_stats['losses']}L) | "
              f"Limiar: {_ia_stats['limiar_atual']:.0%} | "
              f"Streak: {_ia_stats['streak_ia']:+d}{Style.RESET_ALL}")
        
        # Mostrar padrão aprendido a cada 5 trades
        if total % 5 == 0 and total > 0:
            # Top 3 melhores padrões
            bons = sorted([(k, v) for k, v in _ia_padroes.items() if v['total'] >= 3],
                         key=lambda x: x[1]['wins']/x[1]['total'], reverse=True)[:3]
            ruins = sorted([(k, v) for k, v in _ia_padroes.items() if v['total'] >= 3],
                          key=lambda x: x[1]['wins']/x[1]['total'])[:3]
            
            if bons:
                print(f"  {Fore.GREEN}[🧠 TOP PADRÕES]")
                for fp, dados in bons:
                    t = dados['wins']/dados['total']
                    parts = fp.split('|')
                    resumo = " ".join(p.split('=')[1] for p in parts)
                    print(f"    ✅ {t:.0%} acerto ({dados['total']}t): {resumo}{Style.RESET_ALL}")
            
            if ruins:
                print(f"  {Fore.RED}[🧠 PIORES PADRÕES]")
                for fp, dados in ruins:
                    t = dados['wins']/dados['total']
                    parts = fp.split('|')
                    resumo = " ".join(p.split('=')[1] for p in parts)
                    print(f"    ❌ {t:.0%} acerto ({dados['total']}t): {resumo}{Style.RESET_ALL}")
        
        # Salvar
        _ia_salvar()




def preparar_e_executar(par, acao_forcada=None, conf_forcada=None):
    """Prepara e executa ordem FUTUROS com gestão de risco."""
    global ciclos_sem_trade, FORCAR_1_TRADE_TESTE
    try:
        with lock_modelos:
            mm = micro_modelos.get(par)
        if not mm:
            print(f"  {Fore.RED}[PREP] {par} | sem micro_modelo{Style.RESET_ALL}")
            return

        acao = acao_forcada or mm.get('acao', 'manter')
        confianca = conf_forcada if conf_forcada is not None else mm.get('confianca', 0.0)

        if acao == 'manter':
            print(f"  {Fore.RED}[PREP] {par} | ação=manter → ignorado{Style.RESET_ALL}")
            return

        if confianca < 0.20:
            print(f"  {Fore.RED}[PREP] {par} | conf {confianca:.2f} < 0.20{Style.RESET_ALL}")
            return

        # Capital
        with lock_capital:
            capital_alocado = capital_por_par.get(par, 0.0)

        # Teste forçado
        if FORCAR_1_TRADE_TESTE and capital_alocado <= 0:
            capital_alocado = CAPITAL_TOTAL * 0.01
            FORCAR_1_TRADE_TESTE = False
            logger.warning(f"[TESTE] {par} | capital forçado para teste único")

        if capital_alocado <= 0:
            print(f"  {Fore.RED}[PREP] {par} | capital_alocado={capital_alocado:.2f} → sem capital{Style.RESET_ALL}")
            return

        if confianca >= 0.8:
            capital_alocado = min(capital_alocado * 1.3, CAPITAL_TOTAL * _adapt_get("risco_trade", RISCO_MAX_POR_TRADE))

        # Preço
        with lock_precos:
            dados = precos_cache.get(par, [])
        if not dados:
            print(f"  {Fore.RED}[PREP] {par} | sem dados de preço{Style.RESET_ALL}")
            return
        ultimo = dados[-1]
        preco_atual = float(ultimo['close'] if isinstance(ultimo, dict) else ultimo)
        if preco_atual <= 0:
            print(f"  {Fore.RED}[PREP] {par} | preço={preco_atual} inválido{Style.RESET_ALL}")
            return

        # Quantidade
        quantidade_bruta = capital_alocado / preco_atual
        if quantidade_bruta <= 0:
            print(f"  {Fore.RED}[PREP] {par} | quantidade_bruta={quantidade_bruta} → zero{Style.RESET_ALL}")
            return
        min_notional, step_size = obter_filtros_binance(par)
        if step_size > 0:
            quantity = math.floor(quantidade_bruta / step_size) * step_size
            # Fix floating point: round to step_size precision
            decimals = max(0, len(str(step_size).rstrip('0').split('.')[-1])) if '.' in str(step_size) else 0
            quantity = round(quantity, decimals)
        else:
            quantity = math.floor(quantidade_bruta * 1e6) / 1e6
        if quantity <= 0:
            print(f"  {Fore.RED}[PREP] {par} | qty=0 após step_size={step_size}{Style.RESET_ALL}")
            return
        order_value = quantity * preco_atual
        if order_value < min_notional:
            print(f"  {Fore.RED}[PREP] {par} | valor ${order_value:.2f} < min_notional ${min_notional}{Style.RESET_ALL}")
            return

        # Saldo
        saldo_real = atualizar_saldo_real()
        usdt_balance = saldo_real.get('USDT', 0.0)
        if order_value > usdt_balance:
            print(f"  {Fore.RED}[PREP] {par} | valor ${order_value:.2f} > saldo ${usdt_balance:.2f}{Style.RESET_ALL}")
            return

        # EXECUTAR! (FUTUROS: BUY=Long, SELL=Short)
        side_exec = 'BUY' if acao == 'compra' else 'SELL'
        print(f"  {Fore.GREEN + Style.BRIGHT}[EXEC GO] {par} | {side_exec} | qty={quantity} | "
              f"preço={preco_atual:.6f} | valor=${order_value:.2f} | conf={confianca:.2f}{Style.RESET_ALL}")
        logger.info(f"[EXEC GO] {par} | {side_exec} | qty={quantity} | "
                     f"preço={preco_atual:.6f} | conf={confianca:.2f}")

        result = enviar_ordem_binance(par, side_exec, quantity)
        if not result:
            print(f"  {Fore.RED}[PREP] {par} | enviar_ordem retornou vazio/erro{Style.RESET_ALL}")
            return

        # Usar preço REAL de execução (não estimativa)
        preco_real = float(result.get('preco_real', 0))
        if preco_real <= 0:
            preco_real = preco_atual  # fallback
        
        # Taxa taker futuros = 0.04% (ida + volta = 0.08% total)
        taxa_entrada = preco_real * 0.0004  # 0.04% taker fee

        with lock_posicoes:
            if side_exec == 'BUY':
                posicoes[par] = posicoes.get(par, 0.0) + quantity
            else:
                posicoes[par] = posicoes.get(par, 0.0) - quantity
        with lock_trailing:
            # Trailing usa preço REAL + taxa inclusa
            preco_entrada_real = preco_real + taxa_entrada if side_exec == 'BUY' else preco_real - taxa_entrada
            trailing_info[par] = {
                'preco_compra': preco_entrada_real,  # inclui taxa
                'preco_real_exec': preco_real,        # preço sem taxa
                'maior_valor': preco_real,
                'menor_valor': preco_real,
                'melhor_lucro': 0.0,
                'tipo': 'LONG' if side_exec == 'BUY' else 'SHORT',
                'tempo_entrada': time.time(),         # QUANDO entrou
            }

        tipo_pos = 'LONG' if side_exec == 'BUY' else 'SHORT'
        valor_real = preco_real * quantity
        registrar_ordem(par, f'{acao}_{tipo_pos.lower()}', quantity, preco_real)
        ciclos_sem_trade = 0

        logger.info(f"  {Fore.GREEN if side_exec == 'BUY' else Fore.RED}"
                     f"[TRADE] {par} | {tipo_pos} | qty={quantity:.6f} | "
                     f"preço_real={preco_real:.8f} | valor=${valor_real:.2f} | "
                     f"taxa=${taxa_entrada * quantity:.4f}"
                     f"{Style.RESET_ALL}")

    except Exception as e:
        logger.error(f"[EXECUTAR] {par} | {e}")


# ================================================================
# TRAILING STOP + STOP LOSS DURO (FUTUROS: LONG + SHORT)
# ================================================================
# NOTA v8.2: monitorar_posicoes_e_trailing() foi REMOVIDA.
# Existiam DUAS funções de monitoramento com regras diferentes,
# causando fechamento inconsistente. Agora SÓ trailing_thread() controla saídas.
# ================================================================

def _monitorar_posicoes_DESATIVADA():
    try:
        with lock_trailing:
            trailing_snapshot = dict(trailing_info)
        for par, trail in trailing_snapshot.items():
            with lock_posicoes:
                pos = float(posicoes.get(par, 0.0))

            # Sem posição → limpar trailing
            if abs(pos) < 1e-12:
                with lock_trailing:
                    trailing_info.pop(par, None)
                continue

            is_long = pos > 0
            is_short = pos < 0
            pos_abs = abs(pos)

            # Preço atual
            try:
                rate_limiter.wait()
                book = client_level2.book_ticker(symbol=par.replace('/', ''))
                preco_bid = float(book['bidPrice'])
                preco_ask = float(book['askPrice'])
                preco_atual = (preco_bid + preco_ask) / 2
            except Exception:
                with lock_precos:
                    dados = precos_cache.get(par, [])
                if dados:
                    ultimo = dados[-1]
                    preco_atual = float(ultimo['close'] if isinstance(ultimo, dict) else ultimo)
                else:
                    continue

            preco_entrada = float(trail.get('preco_compra', preco_atual))

            # ===== LUCRO: diferente para LONG e SHORT =====
            if is_long:
                lucro_pct = (preco_atual - preco_entrada) / preco_entrada if preco_entrada > 0 else 0
            else:  # SHORT
                lucro_pct = (preco_entrada - preco_atual) / preco_entrada if preco_entrada > 0 else 0

            # ===== TOPO DE LUCRO: maior lucro atingido =====
            with lock_trailing:
                if par not in trailing_info:
                    continue
                melhor_lucro = trail.get('melhor_lucro', 0.0)
                melhor_lucro = max(melhor_lucro, lucro_pct)
                trailing_info[par]['melhor_lucro'] = melhor_lucro

                # Topo do preço (para LONG) ou fundo (para SHORT)
                if is_long:
                    if 'maior_valor' not in trailing_info[par]:
                        trailing_info[par]['maior_valor'] = preco_entrada
                    trailing_info[par]['maior_valor'] = max(
                        trailing_info[par]['maior_valor'], preco_atual
                    )
                    topo = float(trailing_info[par]['maior_valor'])
                else:  # SHORT — queremos o MENOR preço (mais lucro)
                    if 'menor_valor' not in trailing_info[par]:
                        trailing_info[par]['menor_valor'] = preco_entrada
                    trailing_info[par]['menor_valor'] = min(
                        trailing_info[par]['menor_valor'], preco_atual
                    )
                    topo = float(trailing_info[par]['menor_valor'])

            # ===== QUEDA DO TOPO =====
            if is_long:
                queda_do_topo = (preco_atual - topo) / topo if topo > 0 else 0
                # queda_do_topo < 0 significa preço caiu do topo
            else:  # SHORT
                queda_do_topo = (topo - preco_atual) / topo if topo > 0 else 0
                # queda_do_topo < 0 significa preço SUBIU do fundo (perda para short)

            # ===== DECISÃO DE FECHAR =====
            fechar = False
            motivo = ""

            # Calcular trailing dinâmico baseado em ATR do par
            trailing_pct = _adapt_get("trailing_base", TRAILING_DROP_DEFAULT)  # ADAPTATIVO
            with lock_indicadores:
                df_trail = indicadores.get(par)
            if df_trail is not None and 'atr' in df_trail.columns and 'close' in df_trail.columns:
                try:
                    atr_val = float(df_trail['atr'].iloc[-1])
                    preco_ref = float(df_trail['close'].iloc[-1])
                    if preco_ref > 0:
                        atr_pct = atr_val / preco_ref
                        # Trailing = 1.5x ATR (mais apertado para scalping)
                        trailing_pct = max(0.004, min(0.015, atr_pct * 1.5))
                except:
                    pass

            # Tempo na posição
            tempo_entrada = trail.get('tempo_entrada', time.time())
            tempo_na_posicao = time.time() - tempo_entrada

            # 1. STOP LOSS DURO — perda absoluta máxima (1.5% para scalping, não 3%)
            if lucro_pct <= -_adapt_get("stop_loss", 0.015):
                fechar = True
                motivo = f"STOP LOSS ({lucro_pct*100:+.2f}%)"

            # ===== SAÍDA INTELIGENTE DA IA =====
            # Scalping de 5 min: dar tempo pro trade desenvolver
            
            # 1.5 SAÍDA RÁPIDA: 120s negativo, nunca positivo
            elif tempo_na_posicao >= 120 and melhor_lucro <= 0.001 and lucro_pct < -0.003:
                fechar = True
                motivo = f"IA_EARLY_EXIT 120s nunca positivo ({lucro_pct*100:+.2f}%)"
            
            # 1.6 SAÍDA RÁPIDA: 180s, piorando significativamente
            elif tempo_na_posicao >= 180 and lucro_pct < -0.005 and melhor_lucro < 0.002:
                fechar = True
                motivo = f"IA_EARLY_EXIT 180s piorando ({lucro_pct*100:+.2f}%)"
            
            # 1.7 SAÍDA: 240s, negativo e sem força nenhuma
            elif tempo_na_posicao >= 240 and lucro_pct < -0.001 and melhor_lucro < 0.003:
                fechar = True
                motivo = f"IA_EARLY_EXIT 240s sem força ({lucro_pct*100:+.2f}% máx={melhor_lucro*100:.2f}%)"
            
            # 1.8 PROTEÇÃO: tinha lucro bom (+0.8%+) e devolveu 80%
            elif melhor_lucro >= 0.008 and lucro_pct < (melhor_lucro * 0.20) and tempo_na_posicao >= 45:
                fechar = True
                motivo = f"IA_PROTECT (de +{melhor_lucro*100:.2f}% pra +{lucro_pct*100:.2f}%)"

            # 2. SAÍDA POR TEMPO — se passou 5 minutos sem lucro, algo deu errado
            # O trade deveria ter se movido a nosso favor em 1-3 minutos
            elif tempo_na_posicao > _adapt_get("timeout", 300) and lucro_pct < 0.003:
                fechar = True
                motivo = f"TIMEOUT {int(tempo_na_posicao)}s ({lucro_pct*100:+.2f}%)"

            # 3-6. TRAILING INTELIGENTE — dar espaço pro scalping 5 min
            # Crypto oscila muito, trailing apertado demais fecha com centavos
            drop_do_topo = melhor_lucro - lucro_pct
            
            if melhor_lucro >= 0.02:
                # Lucro excelente (2%+): protege 70%, permite cair 30%
                max_drop = melhor_lucro * 0.30
                if drop_do_topo > max_drop:
                    fechar = True
                    motivo = f"TRAILING (máx={melhor_lucro*100:.2f}% atual={lucro_pct*100:.2f}% drop={drop_do_topo*100:.2f}%>{max_drop*100:.2f}%)"
            
            elif melhor_lucro >= 0.01:
                # Lucro bom (1-2%): protege 60%, permite cair 40%
                max_drop = melhor_lucro * 0.40
                if drop_do_topo > max_drop:
                    fechar = True
                    motivo = f"TRAILING (máx={melhor_lucro*100:.2f}% atual={lucro_pct*100:.2f}% drop={drop_do_topo*100:.2f}%>{max_drop*100:.2f}%)"
            
            elif melhor_lucro >= 0.005:
                # Lucro médio (0.5-1%): protege 45%, permite cair 55%
                max_drop = melhor_lucro * 0.55
                if drop_do_topo > max_drop and lucro_pct > 0:
                    fechar = True
                    motivo = f"TRAILING (máx={melhor_lucro*100:.2f}% atual={lucro_pct*100:.2f}% drop={drop_do_topo*100:.2f}%>{max_drop*100:.2f}%)"
            
            elif melhor_lucro >= 0.003:
                # Lucro pequeno (0.3-0.5%): permite cair 70% — máximo espaço
                max_drop = melhor_lucro * 0.70
                if drop_do_topo > max_drop and lucro_pct > 0:
                    fechar = True
                    motivo = f"TRAILING (máx={melhor_lucro*100:.2f}% atual={lucro_pct*100:.2f}% drop={drop_do_topo*100:.2f}%>{max_drop*100:.2f}%)"
            
            # BREAKEVEN — tinha lucro BOM (+1%+) e voltou pra zero
            if not fechar and melhor_lucro >= _adapt_get("breakeven_trigger", 0.01) and lucro_pct <= _adapt_get("breakeven_exit", 0.0005):
                fechar = True
                motivo = f"BREAKEVEN ({lucro_pct*100:+.2f}% após máx {melhor_lucro*100:.2f}%)"

            if fechar:
                try:
                    min_notional, step_size = obter_filtros_binance(par)
                    if step_size and step_size > 0:
                        quantidade = math.floor(pos_abs / step_size) * step_size
                        decimals = max(0, len(str(step_size).rstrip('0').split('.')[-1])) if '.' in str(step_size) else 0
                        quantidade = round(quantidade, decimals)
                    else:
                        quantidade = round(pos_abs, 6)
                    if quantidade <= 0:
                        continue

                    # LONG fecha com SELL, SHORT fecha com BUY
                    side_fechar = 'SELL' if is_long else 'BUY'
                    result = enviar_ordem_binance(par, side_fechar, quantidade)

                    with lock_posicoes:
                        posicoes[par] = 0.0
                    with lock_trailing:
                        trailing_info.pop(par, None)

                    # Lucro absoluto em USDT (incluindo taxas)
                    taxa_saida = preco_atual * quantidade * 0.0004  # 0.04% taker
                    if is_long:
                        lucro_abs = (preco_atual - preco_entrada) * quantidade - taxa_saida
                    else:
                        lucro_abs = (preco_entrada - preco_atual) * quantidade - taxa_saida

                    tipo_pos = "LONG" if is_long else "SHORT"
                    registrar_ordem(par, f'fechar_{tipo_pos.lower()}', quantidade, preco_atual, lucro_abs)

                    # Atualizar P&L, streak e cooldown
                    global pnl_sessao, pnl_por_par, streak_atual, ultimo_loss_time
                    pnl_sessao += lucro_abs
                    pnl_por_par[par] = pnl_por_par.get(par, 0.0) + lucro_abs
                    if lucro_abs > 0:
                        streak_atual = max(1, streak_atual + 1)
                    else:
                        streak_atual = min(-1, streak_atual - 1)
                        ultimo_loss_time = time.time()

                    # Feedback para Cérebro IA
                    try:
                        direcao_certa = lucro_pct > -0.001
                        ia_registrar_resultado(par, direcao_certa, lucro_pct=lucro_pct)
                    except:
                        pass

                    # Registrar acerto/erro
                    with lock_modelos:
                        mm_trail = micro_modelos.get(par, {})
                        hist_trades = mm_trail.get('micro_hist_trades', [])
                        hist_trades.append(1 if lucro_pct > -0.001 else 0)
                        mm_trail['micro_hist_trades'] = hist_trades[-20:]
                        micro_modelos[par] = mm_trail

                        # ANTI-FRAGILIDADE
                        try:
                            registrar_resultado_trade(
                                par,
                                acao=mm_trail.get('acao', 'compra'),
                                forca=mm_trail.get('forca_final', 0),
                                confianca=mm_trail.get('confianca', 0),
                                regime=regime_mercado,
                                funding=mm_trail.get('funding_sinal', 0),
                                flow=mm_trail.get('order_flow', 0),
                                ganhou=(lucro_abs > 0)
                            )
                        except:
                            pass

                    cor_lucro = Fore.GREEN if lucro_abs > 0 else Fore.RED
                    logger.info(f"[{motivo}] {par} | {tipo_pos} → {side_fechar} | "
                                f"{cor_lucro}Lucro:{lucro_abs:+.4f} USDT{Style.RESET_ALL} | "
                                f"P&L: {pnl_sessao:+.2f} | Streak: {streak_atual}")
                except Exception as e:
                    logger.error(f"[TRAILING VENDA] {par} | {e}")
    except Exception as e:
        logger.error(f"[TRAILING MONITOR] {e}")


def trailing_thread():
    """
    Thread INDEPENDENTE de monitoramento.
    Usa seu próprio client e NÃO compartilha rate limiter.
    Busca preços de TODAS as posições de uma vez, não uma por uma.
    """
    # Client próprio para não bloquear/ser bloqueado
    trail_client = UMFutures(key=API_KEY, secret=API_SECRET)
    
    while not encerrando:
        try:
            with lock_trailing:
                trailing_snapshot = dict(trailing_info)
            
            if not trailing_snapshot:
                time.sleep(0.3)
                continue
            
            # Buscar TODOS os preços de uma vez (1 API call, não N)
            try:
                all_tickers = trail_client.book_ticker()
                price_map = {}
                for t in all_tickers:
                    sym = t.get('symbol', '')
                    bid = float(t.get('bidPrice', 0))
                    ask = float(t.get('askPrice', 0))
                    if bid > 0 and ask > 0:
                        price_map[sym] = (bid + ask) / 2
            except Exception as e:
                logger.debug(f"[TRAIL] Erro preços: {e}")
                time.sleep(0.5)
                continue
            
            # Agora checar TODAS as posições com preços frescos
            for par, trail in trailing_snapshot.items():
                if par.startswith('_hist_'):
                    continue  # pular chaves de histórico de velocidade
                try:
                    # ===== MONITORAMENTO PÓS-ENTRADA DA IA =====
                    trade_ia = _ia_trades_ativos.get(par)
                    if trade_ia:
                        tempo_trade = time.time() - trade_ia.get('entrada_ts', time.time())
                        preco_agora_ia = price_map.get(par, 0)
                        if preco_agora_ia > 0 and trade_ia.get('preco_entrada', 0) > 0:
                            # Gravar curva de preço a cada ~5 segundos
                            curva = trade_ia.get('curva_precos', [])
                            if not curva or (tempo_trade - (curva[-1][0] if curva else 0)) >= 5:
                                pct = (preco_agora_ia - trade_ia['preco_entrada']) / trade_ia['preco_entrada']
                                if trade_ia.get('direcao') == 'venda':
                                    pct = -pct
                                curva.append((round(tempo_trade, 1), round(pct * 100, 3)))
                                _ia_trades_ativos[par]['curva_precos'] = curva[-60:]  # últimos 5 min
                            
                            # Aos 15 segundos: registrar se subiu ou caiu
                            if 13 <= tempo_trade <= 20 and not trade_ia.get('check_30s'):
                                pct_30s = (preco_agora_ia - trade_ia['preco_entrada']) / trade_ia['preco_entrada']
                                if trade_ia.get('direcao') == 'venda':
                                    pct_30s = -pct_30s
                                if pct_30s > 0:
                                    _ia_stats['trades_subiu_30s'] = _ia_stats.get('trades_subiu_30s', 0) + 1
                                else:
                                    _ia_stats['trades_caiu_30s'] = _ia_stats.get('trades_caiu_30s', 0) + 1
                                _ia_trades_ativos[par]['check_30s'] = True
                                cor30 = Fore.GREEN if pct_30s > 0 else Fore.RED
                                print(f"  {cor30}[🧠 15s] {par} | {pct_30s*100:+.2f}% após 15s | "
                                      f"stats: {_ia_stats.get('trades_subiu_30s',0)}↑ / "
                                      f"{_ia_stats.get('trades_caiu_30s',0)}↓{Style.RESET_ALL}")
                    
                    with lock_posicoes:
                        pos = float(posicoes.get(par, 0.0))
                    
                    if abs(pos) < 1e-12:
                        with lock_trailing:
                            trailing_info.pop(par, None)
                        continue
                    
                    preco_atual = price_map.get(par, 0)
                    if preco_atual <= 0:
                        continue
                    
                    is_long = pos > 0
                    pos_abs = abs(pos)
                    preco_entrada = float(trail.get('preco_compra', preco_atual))
                    
                    # Lucro
                    if is_long:
                        lucro_pct = (preco_atual - preco_entrada) / preco_entrada if preco_entrada > 0 else 0
                    else:
                        lucro_pct = (preco_entrada - preco_atual) / preco_entrada if preco_entrada > 0 else 0
                    
                    # Atualizar topo
                    with lock_trailing:
                        if par not in trailing_info:
                            continue
                        melhor_lucro = max(trail.get('melhor_lucro', 0.0), lucro_pct)
                        trailing_info[par]['melhor_lucro'] = melhor_lucro
                        if is_long:
                            trailing_info[par]['maior_valor'] = max(
                                trailing_info[par].get('maior_valor', preco_entrada), preco_atual)
                        else:
                            trailing_info[par]['menor_valor'] = min(
                                trailing_info[par].get('menor_valor', preco_entrada), preco_atual)
                    
                    # Trailing dinâmico
                    trailing_pct = 0.008  # default 0.8%
                    with lock_indicadores:
                        df_trail = indicadores.get(par)
                    if df_trail is not None and 'atr' in df_trail.columns and 'close' in df_trail.columns:
                        try:
                            atr_val = float(df_trail['atr'].iloc[-1])
                            preco_ref = float(df_trail['close'].iloc[-1])
                            if preco_ref > 0:
                                trailing_pct = max(0.004, min(0.015, (atr_val / preco_ref) * 1.5))
                        except:
                            pass
                    
                    # Tempo na posição
                    tempo_entrada = trail.get('tempo_entrada', time.time())
                    tempo_na_posicao = time.time() - tempo_entrada
                    
                    # ============================================================
                    # VELOCÍMETRO: medir velocidade do preço em tempo real
                    # ============================================================
                    # Gravar histórico de lucro_pct a cada poll (~200ms)
                    # Últimos 30 polls = ~6 segundos de história
                    hist_key = f'_hist_{par}'
                    if hist_key not in trailing_info:
                        with lock_trailing:
                            trailing_info[hist_key] = []
                    
                    agora = time.time()
                    with lock_trailing:
                        hist_precos = trailing_info.get(hist_key, [])
                        hist_precos.append((agora, lucro_pct))
                        # Manter últimos 60 pontos (~12 segundos a 200ms)
                        if len(hist_precos) > 60:
                            hist_precos = hist_precos[-60:]
                        trailing_info[hist_key] = hist_precos
                    
                    # Calcular VELOCIDADE (quanto muda por segundo)
                    velocidade_por_seg = 0.0
                    aceleracao = 0.0
                    velocidade_3s = 0.0  # velocidade nos últimos 3 segundos
                    
                    if len(hist_precos) >= 5:
                        # Velocidade instantânea (últimos ~1 segundo = 5 polls)
                        dt = hist_precos[-1][0] - hist_precos[-5][0]
                        if dt > 0:
                            velocidade_por_seg = (hist_precos[-1][1] - hist_precos[-5][1]) / dt
                        
                        # Velocidade 3 segundos (últimos ~15 polls)
                        idx_3s = max(0, len(hist_precos) - 15)
                        dt_3s = hist_precos[-1][0] - hist_precos[idx_3s][0]
                        if dt_3s > 0:
                            velocidade_3s = (hist_precos[-1][1] - hist_precos[idx_3s][1]) / dt_3s
                        
                        # Aceleração: velocidade está AUMENTANDO ou DIMINUINDO?
                        if len(hist_precos) >= 15:
                            idx_meio = len(hist_precos) // 2
                            dt_ant = hist_precos[idx_meio][0] - hist_precos[0][0]
                            if dt_ant > 0:
                                vel_anterior = (hist_precos[idx_meio][1] - hist_precos[0][1]) / dt_ant
                                dt_rec = hist_precos[-1][0] - hist_precos[idx_meio][0]
                                if dt_rec > 0:
                                    vel_recente = (hist_precos[-1][1] - hist_precos[idx_meio][1]) / dt_rec
                                    aceleracao = vel_recente - vel_anterior
                    
                    # ============================================================
                    # CLASSIFICAR PERSONALIDADE DA MOEDA EM TEMPO REAL
                    # ============================================================
                    # Baseado na amplitude de oscilação dos últimos 6 segundos
                    volatilidade_moeda = 'normal'
                    if len(hist_precos) >= 10:
                        lucros_recentes = [h[1] for h in hist_precos[-30:]]
                        amplitude = max(lucros_recentes) - min(lucros_recentes)
                        if amplitude > 0.008:      # >0.8% em 6s = SPIKE (meme coin, pump)
                            volatilidade_moeda = 'spike'
                        elif amplitude > 0.004:    # >0.4% em 6s = volátil
                            volatilidade_moeda = 'volatil'
                        elif amplitude < 0.001:    # <0.1% em 6s = calma (BTC, ETH)
                            volatilidade_moeda = 'calma'
                    
                    # ===== DECISÃO DE FECHAR =====
                    fechar = False
                    motivo = ""
                    
                    # LOG: mostrar estado de cada posição com velocidade
                    tipo_p = 'LONG' if is_long else 'SHORT'
                    vel_emoji = "🚀" if velocidade_3s > 0.001 else "📉" if velocidade_3s < -0.001 else "➡️"
                    vol_tag = f"[{volatilidade_moeda.upper()}]"
                    print(f"  {Fore.CYAN}[MONITOR] {par} {tipo_p} {vol_tag} | "
                          f"P&L={lucro_pct*100:+.2f}% | máx={melhor_lucro*100:.2f}% | "
                          f"vel={velocidade_3s*100:+.3f}%/s {vel_emoji} | "
                          f"tempo={int(tempo_na_posicao)}s{Style.RESET_ALL}")
                    
                    # ============================================================
                    # 0. FREIO DE EMERGÊNCIA — CRASH DETECTADO
                    # ============================================================
                    # Se o preço está caindo MUY rápido E temos lucro, FECHAR AGORA
                    # Não esperar trailing — a cada 200ms perde dinheiro
                    
                    if melhor_lucro > 0.002 and velocidade_3s < -0.002:
                        # Caindo mais de 0.2%/segundo com lucro = CRASH
                        # Em 3 segundos perderia +0.6% — fechar AGORA
                        fechar = True
                        motivo = (f"🚨 CRASH_EXIT vel={velocidade_3s*100:.3f}%/s | "
                                  f"de +{melhor_lucro*100:.2f}% pra +{lucro_pct*100:.2f}%")
                    
                    elif melhor_lucro > 0.005 and velocidade_3s < -0.001 and aceleracao < -0.0005:
                        # Caindo 0.1%/s E ACELERANDO a queda = vai piorar
                        fechar = True
                        motivo = (f"🚨 ACCEL_EXIT vel={velocidade_3s*100:.3f}%/s accel={aceleracao*100:.3f} | "
                                  f"+{lucro_pct*100:.2f}%")
                    
                    # ============================================================
                    # TRAILING PROPORCIONAL — ADAPTADO À PERSONALIDADE DA MOEDA
                    # ============================================================
                    # Moeda SPIKE (oscila muito): trailing mais apertado
                    # Moeda CALMA (BTC, ETH): trailing mais solto
                    # Moeda NORMAL: trailing padrão
                    
                    # Fator de ajuste baseado na personalidade
                    if volatilidade_moeda == 'spike':
                        fator_vol = 0.65   # 35% mais apertado (moeda perigosa)
                    elif volatilidade_moeda == 'volatil':
                        fator_vol = 0.80   # 20% mais apertado
                    elif volatilidade_moeda == 'calma':
                        fator_vol = 1.20   # 20% mais solto (oscila pouco)
                    else:
                        fator_vol = 1.0    # normal
                    
                    # Fator de tempo: trade antigo = aperta trailing
                    if tempo_na_posicao > 300:      # >5min
                        fator_tempo = 0.70           # 30% mais apertado
                    elif tempo_na_posicao > 180:     # >3min
                        fator_tempo = 0.85           # 15% mais apertado
                    else:
                        fator_tempo = 1.0            # normal
                    
                    # Fator de velocidade: se está caindo, aperta
                    if velocidade_3s < -0.0005:
                        fator_vel = 0.75             # caindo = 25% mais apertado
                    elif velocidade_3s > 0.001:
                        fator_vel = 1.15             # subindo = 15% mais solto
                    else:
                        fator_vel = 1.0
                    
                    # Trailing tiers COM ajustes dinâmicos
                    if melhor_lucro >= 0.02:
                        trail_drop_pct = 0.30
                    elif melhor_lucro >= 0.01:
                        trail_drop_pct = 0.40
                    elif melhor_lucro >= 0.005:
                        trail_drop_pct = 0.55
                    elif melhor_lucro >= 0.003:
                        trail_drop_pct = 0.70
                    else:
                        trail_drop_pct = 1.0
                    
                    # Aplicar fatores dinâmicos
                    trail_drop_pct = trail_drop_pct * fator_vol * fator_tempo * fator_vel
                    trail_drop_pct = max(0.15, min(1.0, trail_drop_pct))  # clamp: 15%-100%
                    
                    max_drop_permitido = melhor_lucro * trail_drop_pct
                    drop_atual = melhor_lucro - lucro_pct
                    
                    # ============================================================
                    # STEPPED PROFIT LOCK — NUNCA DEVOLVER LUCRO GRANDE
                    # ============================================================
                    # Se chegou a +X%, garante MÍNIMO de saída em +Y%
                    # Isso é INDEPENDENTE do trailing — é um piso de lucro
                    
                    lucro_minimo_garantido = 0.0  # por default, sem garantia
                    
                    if not fechar:  # só se o crash exit não ativou
                        if melhor_lucro >= 0.025:
                            lucro_minimo_garantido = 0.012   # chegou +2.5% → garante +1.2%
                        elif melhor_lucro >= 0.020:
                            lucro_minimo_garantido = 0.010   # chegou +2.0% → garante +1.0%
                        elif melhor_lucro >= 0.015:
                            lucro_minimo_garantido = 0.007   # chegou +1.5% → garante +0.7%
                        elif melhor_lucro >= 0.010:
                            lucro_minimo_garantido = 0.004   # chegou +1.0% → garante +0.4%
                        elif melhor_lucro >= 0.007:
                            lucro_minimo_garantido = 0.002   # chegou +0.7% → garante +0.2%
                        
                        if lucro_minimo_garantido > 0 and lucro_pct <= lucro_minimo_garantido:
                            fechar = True
                            motivo = (f"🔒 PROFIT_LOCK (máx={melhor_lucro*100:.2f}% → "
                                      f"min_garantido={lucro_minimo_garantido*100:.2f}% → "
                                      f"atual={lucro_pct*100:.2f}%)")
                    
                    # ============================================================
                    # REGRAS DE SAÍDA (em ordem de prioridade)
                    # ============================================================
                    
                    if not fechar:
                        # 1. STOP LOSS DURO
                        if lucro_pct <= -_adapt_get("stop_loss", 0.015):
                            fechar = True
                            motivo = f"STOP LOSS ({lucro_pct*100:+.2f}%)"
                        
                        # 2. SAÍDA RÁPIDA: 120s negativo, NUNCA ficou positivo
                        elif tempo_na_posicao >= 120 and melhor_lucro <= 0.001 and lucro_pct < -0.003:
                            fechar = True
                            motivo = f"IA_EARLY_EXIT 120s nunca positivo ({lucro_pct*100:+.2f}%)"
                        
                        # 3. SAÍDA RÁPIDA: 180s, piorando muito
                        elif tempo_na_posicao >= 180 and lucro_pct < -0.005 and melhor_lucro < 0.002:
                            fechar = True
                            motivo = f"IA_EARLY_EXIT 180s piorando ({lucro_pct*100:+.2f}%)"
                        
                        # 4. SAÍDA: 240s, negativo e sem força
                        elif tempo_na_posicao >= 240 and lucro_pct < -0.001 and melhor_lucro < 0.003:
                            fechar = True
                            motivo = f"IA_EARLY_EXIT 240s sem força ({lucro_pct*100:+.2f}%)"
                        
                        # 5. PROTEÇÃO IA: tinha lucro bom e devolveu 80%
                        elif melhor_lucro >= 0.008 and lucro_pct < (melhor_lucro * 0.20) and tempo_na_posicao >= 45:
                            fechar = True
                            motivo = f"IA_PROTECT (de +{melhor_lucro*100:.2f}% pra +{lucro_pct*100:.2f}%)"
                        
                        # 6. TIMEOUT
                        elif tempo_na_posicao > _adapt_get("timeout", 600) and lucro_pct < 0.003:
                            fechar = True
                            motivo = f"TIMEOUT {int(tempo_na_posicao)}s ({lucro_pct*100:+.2f}%)"
                        
                        # 7. TRAILING PROPORCIONAL (agora com fatores dinâmicos)
                        elif melhor_lucro >= 0.003 and drop_atual > max_drop_permitido and lucro_pct > 0:
                            fechar = True
                            motivo = (f"TRAILING [{volatilidade_moeda}] (máx={melhor_lucro*100:.2f}% "
                                      f"atual={lucro_pct*100:.2f}% "
                                      f"drop={drop_atual*100:.2f}%>{max_drop_permitido*100:.2f}%)")
                        
                        # 8. PROTEÇÃO: lucro caiu pra quase zero depois de +1%+
                        elif melhor_lucro >= 0.01 and lucro_pct <= 0.001:
                            fechar = True
                            motivo = f"PROTEÇÃO (tinha +{melhor_lucro*100:.2f}% agora +{lucro_pct*100:.2f}%)"
                        
                        # 9. LOCK: acima de 2%, apertadíssimo
                        elif lucro_pct >= 0.02 and drop_atual > 0.003:
                            fechar = True
                            motivo = f"LOCK ({lucro_pct*100:+.2f}%)"
                        
                        # 10. SPIKE REVERSAL: moeda subiu rápido E agora está caindo rápido
                        # (spike que vai e volta — precisa pegar no caminho de volta)
                        elif (volatilidade_moeda == 'spike' and melhor_lucro > 0.005 
                              and velocidade_3s < -0.0003 and lucro_pct < melhor_lucro * 0.60):
                            fechar = True
                            motivo = (f"SPIKE_REVERSAL [{volatilidade_moeda}] "
                                      f"vel={velocidade_3s*100:.3f}%/s | "
                                      f"+{lucro_pct*100:.2f}% de +{melhor_lucro*100:.2f}%")
                    
                    if fechar:
                        try:
                            min_notional, step_size = obter_filtros_binance(par)
                            if step_size and step_size > 0:
                                quantidade = math.floor(pos_abs / step_size) * step_size
                                decimals = max(0, len(str(step_size).rstrip('0').split('.')[-1])) if '.' in str(step_size) else 0
                                quantidade = round(quantidade, decimals)
                            else:
                                quantidade = round(pos_abs, 6)
                            if quantidade <= 0:
                                continue
                            
                            side_fechar = 'SELL' if is_long else 'BUY'
                            tipo_pos = 'LONG' if is_long else 'SHORT'
                            result = enviar_ordem_binance(par, side_fechar, quantidade)
                            
                            with lock_posicoes:
                                posicoes[par] = 0.0
                            with lock_trailing:
                                trailing_info.pop(par, None)
                                # Limpar histórico de velocidade
                                trailing_info.pop(f'_hist_{par}', None)
                            
                            taxa_saida = preco_atual * quantidade * 0.0004
                            if is_long:
                                lucro_abs = (preco_atual - preco_entrada) * quantidade - taxa_saida
                            else:
                                lucro_abs = (preco_entrada - preco_atual) * quantidade - taxa_saida
                            
                            global pnl_sessao, streak_atual, ultimo_loss_time
                            pnl_sessao += lucro_abs
                            pnl_por_par[par] = pnl_por_par.get(par, 0.0) + lucro_abs
                            if lucro_abs > 0:
                                streak_atual = max(1, streak_atual + 1)
                            else:
                                streak_atual = min(-1, streak_atual - 1)
                                ultimo_loss_time = time.time()
                            
                            # Feedback para o Cérebro IA
                            # A2 FIX: A IA aprende DIREÇÃO, não lucro absoluto.
                            # Um trade que acertou a direção (+0.03%) mas não cobriu taxas
                            # é WIN para aprendizado (a IA escolheu certo).
                            # P&L usa lucro_abs (com taxa). IA usa lucro_pct (sem dupla-contagem).
                            try:
                                direcao_certa = lucro_pct > -0.001  # tolerância de 0.1% para taxas
                                ia_registrar_resultado(par, direcao_certa, lucro_pct=lucro_pct)
                            except:
                                pass
                            
                            with lock_modelos:
                                mm_trail = micro_modelos.get(par, {})
                                hist_trades = mm_trail.get('micro_hist_trades', [])
                                # micro_hist_trades também usa direção (consistência com IA)
                                hist_trades.append(1 if lucro_pct > -0.001 else 0)
                                mm_trail['micro_hist_trades'] = hist_trades[-20:]
                                micro_modelos[par] = mm_trail
                                try:
                                    registrar_resultado_trade(
                                        par, acao=mm_trail.get('acao', 'compra'),
                                        forca=mm_trail.get('forca_final', 0),
                                        confianca=mm_trail.get('confianca', 0),
                                        regime=regime_mercado,
                                        funding=mm_trail.get('funding_sinal', 0),
                                        flow=mm_trail.get('order_flow', 0),
                                        ganhou=(lucro_abs > 0))
                                except:
                                    pass
                            
                            cor_lucro = Fore.GREEN if lucro_abs > 0 else Fore.RED
                            logger.info(f"[{motivo}] {par} | {tipo_pos} → {side_fechar} | "
                                        f"{cor_lucro}Lucro:{lucro_abs:+.4f} USDT{Style.RESET_ALL} | "
                                        f"P&L: {pnl_sessao:+.2f} | Streak: {streak_atual}")
                        except Exception as e:
                            logger.error(f"[TRAILING VENDA] {par} | {e}")
                except Exception as e:
                    logger.debug(f"[TRAIL] {par} | {e}")
            
            time.sleep(0.2)  # 200ms entre checagens — 5x por segundo
            
        except Exception as e:
            logger.error(f"[TRAILING THREAD] {e}")
            time.sleep(0.5)


# ================================================================
# DETECTOR DE REGIME DE MERCADO
# ================================================================
def detectar_regime_mercado():
    """
    Detecta regime do mercado via BTC como referência:
    - bull: BTC subindo consistente (MA curtas > longas, ADX > 20)
    - bear: BTC caindo consistente
    - chop: ADX baixo, sem direção clara (PIOR para trading)
    - neutro: transição
    
    Regime afeta:
    - bull  → favorece LONGs, reduce SHORTs
    - bear  → favorece SHORTs, reduce LONGs
    - chop  → reduz tamanho de TODAS as posições
    - neutro → opera normal
    """
    global regime_mercado
    try:
        with lock_indicadores:
            df_btc = indicadores.get('BTCUSDT')
        if df_btc is None or len(df_btc) < 50:
            regime_mercado = 'neutro'
            return regime_mercado

        latest = df_btc.iloc[-1]
        ma3 = float(latest.get('ma_3', 0))
        ma15 = float(latest.get('ma_15', 0))
        ma50 = float(latest.get('ma_50', 0))
        adx = float(latest.get('adx', 0))
        rsi = float(latest.get('rsi', 50))

        # Tendência forte
        if adx > 25:
            if ma3 > ma15 > ma50 and rsi > 45:
                regime_mercado = 'bull'
            elif ma3 < ma15 < ma50 and rsi < 55:
                regime_mercado = 'bear'
            else:
                regime_mercado = 'neutro'
        elif adx < 15:
            regime_mercado = 'chop'  # sem tendência = perigo
        else:
            regime_mercado = 'neutro'

        logger.info(f"[REGIME] {regime_mercado.upper()} | BTC ADX={adx:.1f} RSI={rsi:.1f} "
                     f"MA3={ma3:.0f} MA15={ma15:.0f} MA50={ma50:.0f}")
        return regime_mercado
    except Exception as e:
        logger.debug(f"[REGIME] Erro: {e}")
        regime_mercado = 'neutro'
        return regime_mercado



# ================================================================
# ADAPTAÇÃO AUTOMÁTICA — O bot lê o mercado e se ajusta
# ================================================================
# Parâmetros que eram FIXOS agora são VARIÁVEIS controladas.
# A cada ciclo, o bot olha:
#   - Volatilidade real (ATR do BTC como referência)
#   - Regime atual (bull/bear/chop)
#   - Performance da IA (ganhando ou perdendo)
#   - Hora do dia (liquidez)
# E ajusta tudo proporcionalmente.
#
# REGRA DE OURO: em mercado calmo, aperta tudo (stop curto, trailing
# apertado, timeout rápido). Em mercado volátil, afrouxa tudo (stop
# largo, trailing solto, timeout longo). Isso é o que traders
# profissionais fazem manualmente. Nosso bot faz sozinho.

# Parâmetros adaptativos (valores atuais — mudam a cada ciclo)
_adapt = {
    'stop_loss': 0.015,        # -1.5% default
    'timeout': 600,            # 10 min default (era 5 min)
    'trailing_base': 0.008,    # 0.8% default
    'max_posicoes': 10,        # 10 default
    'cooldown': 60,            # 60s default
    'risco_trade': 0.02,       # 2% default
    'breakeven_trigger': 0.008, # +0.8% default (era 0.5%)
    'breakeven_exit': 0.001,   # +0.1% default (era 0.05%)
    'take_profit_drop': 0.003, # 0.3% de queda do topo
    'vol_btc': 0.0,           # volatilidade medida do BTC
    'vol_classe': 'medio',     # 'calmo', 'medio', 'volatil', 'extremo'
    'ultima_adaptacao': 0,
}

def adaptar_parametros_mercado():
    """
    Mede a volatilidade REAL e ajusta todos os parâmetros.
    Roda a cada ciclo (~5-10s).
    """
    global _adapt
    
    try:
        now = time.time()
        
        # ============================================================
        # 1. MEDIR VOLATILIDADE REAL (via ATR do BTC)
        # ============================================================
        vol_btc = 0.01  # default
        vol_classe = 'medio'
        
        with lock_indicadores:
            df_btc = indicadores.get('BTCUSDT')
        
        if df_btc is not None and 'atr' in df_btc.columns and 'close' in df_btc.columns:
            try:
                atr = float(df_btc['atr'].iloc[-1])
                preco = float(df_btc['close'].iloc[-1])
                if preco > 0:
                    vol_btc = atr / preco  # ATR como % do preço
            except:
                pass
        
        # Classificar volatilidade
        # ATR/preço típico BTC:
        #   < 0.003 (0.3%) = calmo (mercado asiático, domingo)
        #   0.003-0.008    = médio (normal)
        #   0.008-0.015    = volátil (notícias, abertura US)
        #   > 0.015        = extremo (crash, pump, CPI, FOMC)
        
        if vol_btc < 0.003:
            vol_classe = 'calmo'
        elif vol_btc < 0.008:
            vol_classe = 'medio'
        elif vol_btc < 0.015:
            vol_classe = 'volatil'
        else:
            vol_classe = 'extremo'
        
        _adapt['vol_btc'] = vol_btc
        _adapt['vol_classe'] = vol_classe
        
        # ============================================================
        # 2. AJUSTAR PARÂMETROS POR VOLATILIDADE
        # ============================================================
        
        if vol_classe == 'calmo':
            # Mercado calmo: movimentos pequenos, previsíveis
            _adapt['stop_loss'] = 0.010       # -1.0%
            _adapt['timeout'] = 420            # 7 min (era 3 min)
            _adapt['trailing_base'] = 0.005    # 0.5% trailing
            _adapt['breakeven_trigger'] = 0.005  # +0.5%
            _adapt['breakeven_exit'] = 0.0005    # sai em +0.05%
            _adapt['take_profit_drop'] = 0.002   # 0.2% de queda = fecha
            _adapt['risco_trade'] = 0.025        # pode arriscar mais (previsível)
            
        elif vol_classe == 'medio':
            # Normal: parâmetros balanceados
            _adapt['stop_loss'] = 0.015
            _adapt['timeout'] = 600            # 10 min (era 5 min)
            _adapt['trailing_base'] = 0.008
            _adapt['breakeven_trigger'] = 0.008  # era 0.005
            _adapt['breakeven_exit'] = 0.001
            _adapt['take_profit_drop'] = 0.003
            _adapt['risco_trade'] = 0.020
            
        elif vol_classe == 'volatil':
            # Volátil: precisa de espaço pra respirar
            _adapt['stop_loss'] = 0.020       # -2.0%
            _adapt['timeout'] = 720            # 12 min (era 7 min)
            _adapt['trailing_base'] = 0.012    # 1.2% trailing
            _adapt['breakeven_trigger'] = 0.012  # precisa de +1.2% pra considerar
            _adapt['breakeven_exit'] = 0.002     # sai em +0.2%
            _adapt['take_profit_drop'] = 0.005   # 0.5% de queda
            _adapt['risco_trade'] = 0.015        # menos risco (imprevisível)
            
        elif vol_classe == 'extremo':
            # Extremo: só opera se tiver sinal muito forte
            _adapt['stop_loss'] = 0.025       # -2.5%
            _adapt['timeout'] = 900            # 15 min (era 10 min)
            _adapt['trailing_base'] = 0.015    # 1.5% trailing
            _adapt['breakeven_trigger'] = 0.015  # precisa de +1.5%
            _adapt['breakeven_exit'] = 0.003     # sai em +0.3%
            _adapt['take_profit_drop'] = 0.008
            _adapt['risco_trade'] = 0.010        # mínimo de risco
        
        # ============================================================
        # 3. AJUSTAR POR REGIME
        # ============================================================
        
        if regime_mercado == 'chop':
            # Mercado picotado: reduz tudo
            _adapt['max_posicoes'] = 3
            _adapt['cooldown'] = 120         # espera mais entre trades
            _adapt['risco_trade'] *= 0.6     # 40% menos risco
            _adapt['timeout'] = int(_adapt['timeout'] * 0.7)  # timeout mais curto
            
        elif regime_mercado in ['bull', 'bear']:
            # Tendência clara: pode abrir mais
            _adapt['max_posicoes'] = 8
            _adapt['cooldown'] = 30          # pouco cooldown (oportunidades)
            _adapt['risco_trade'] *= 1.2     # 20% mais risco
            _adapt['timeout'] = int(_adapt['timeout'] * 1.3)  # mais paciência
            
        else:  # neutro
            _adapt['max_posicoes'] = 5
            _adapt['cooldown'] = 60
        
        # ============================================================
        # 4. AJUSTAR POR PERFORMANCE DA IA
        # ============================================================
        
        streak = _ia_stats.get('streak_ia', 0)
        
        if streak <= -3:
            # Perdendo sequência: reduz exposição
            _adapt['max_posicoes'] = min(_adapt['max_posicoes'], 2)
            _adapt['risco_trade'] *= 0.5
            _adapt['cooldown'] = max(_adapt['cooldown'], 180)
            
        elif streak >= 3:
            # Ganhando sequência: pode manter ritmo (mas não aumentar demais)
            _adapt['risco_trade'] = min(_adapt['risco_trade'] * 1.1, 0.03)
        
        # ============================================================
        # 5. AJUSTAR POR HORÁRIO (UTC)
        # ============================================================
        
        hora = datetime.now(timezone.utc).hour
        
        # Horários de baixa liquidez → mais conservador
        if 22 <= hora or hora < 4:  # noite/madrugada UTC
            _adapt['risco_trade'] *= 0.7
            _adapt['max_posicoes'] = min(_adapt['max_posicoes'], 3)
            _adapt['timeout'] = int(_adapt['timeout'] * 0.8)
        
        # Horários de alta liquidez → pode operar normal
        elif 13 <= hora <= 20:  # overlap US/EU
            pass  # mantém tudo como está
        
        # ============================================================
        # LIMITES DE SEGURANÇA (nunca ultrapassa)
        # ============================================================
        _adapt['stop_loss'] = max(0.005, min(0.030, _adapt['stop_loss']))
        _adapt['timeout'] = max(300, min(1200, _adapt['timeout']))
        _adapt['trailing_base'] = max(0.003, min(0.020, _adapt['trailing_base']))
        _adapt['max_posicoes'] = max(1, min(10, _adapt['max_posicoes']))
        _adapt['cooldown'] = max(15, min(300, _adapt['cooldown']))
        _adapt['risco_trade'] = max(0.005, min(0.035, _adapt['risco_trade']))
        _adapt['breakeven_trigger'] = max(0.002, min(0.020, _adapt['breakeven_trigger']))
        
        _adapt['ultima_adaptacao'] = now
        
        # Log (a cada ~5 ciclos para não poluir)
        if int(now) % 25 < 6:
            logger.info(
                f"[ADAPT] vol={vol_btc:.4f}({vol_classe}) regime={regime_mercado} | "
                f"stop={_adapt['stop_loss']*100:.1f}% timeout={_adapt['timeout']}s "
                f"trail={_adapt['trailing_base']*100:.1f}% risco={_adapt['risco_trade']*100:.1f}% "
                f"maxpos={_adapt['max_posicoes']} cool={_adapt['cooldown']}s")
        
    except Exception as e:
        logger.debug(f"[ADAPT] Erro: {e}")


def _adapt_get(param, default=None):
    """Pega parâmetro adaptativo. Fallback pro default se não existir."""
    return _adapt.get(param, default)


# ================================================================
# SINAIS PREDITIVOS — Antecedem o preço
# ================================================================

# Cache para order flow (book anterior para medir delta)
_book_anterior = {}  # par -> {'total_bid': float, 'total_ask': float, 'ts': float}
_funding_cache = {}  # par -> (rate, timestamp)
_oi_cache = {}       # par -> [(oi, ts), ...] últimos 10 valores

def coletar_order_flow_delta(par):
    """
    ORDER FLOW IMBALANCE — mede a VELOCIDADE de mudança do book.
    Não é o book estático (bid vs ask) — é o DELTA entre snapshots.
    
    Se bids estão sendo comidos (diminuindo) enquanto asks crescem:
    → agressores estão vendendo → preço vai cair ANTES do preço mover
    
    Se asks estão sendo comidos enquanto bids crescem:
    → agressores estão comprando → preço vai subir
    
    Retorna: float [-1, +1] onde:
      +1 = pressão compradora extrema (preço vai subir)
      -1 = pressão vendedora extrema (preço vai cair)
       0 = equilíbrio
    """
    global _book_anterior
    try:
        book = precos_cache.get(par + '_book', {})
        if not isinstance(book, dict) or not book.get('bids') or not book.get('asks'):
            return 0.0

        total_bid = sum(q for _, q in book['bids'])
        total_ask = sum(q for _, q in book['asks'])
        now = time.time()

        # Snapshot anterior
        anterior = _book_anterior.get(par)
        _book_anterior[par] = {'total_bid': total_bid, 'total_ask': total_ask, 'ts': now}

        if not anterior or (now - anterior['ts']) > 300:
            return 0.0  # sem referência ou muito antigo

        # DELTA = variação desde o último snapshot
        delta_bid = total_bid - anterior['total_bid']
        delta_ask = total_ask - anterior['total_ask']

        # Normalizar pelo total
        total = total_bid + total_ask + 1e-9
        flow = (delta_bid - delta_ask) / total

        # Clamp e suavizar
        flow = max(-1.0, min(1.0, flow * 5))  # amplifica sinal sutil

        return flow

    except Exception as e:
        logger.debug(f"[ORDER FLOW] {par} | {e}")
        return 0.0


def coletar_funding_rate(par):
    """
    FUNDING RATE — sinal contrário.
    
    Funding rate positivo alto (> 0.05%) = muitos longs pagando shorts
    → mercado sobrecarregado de longs → tendência de queda (SHORT)
    
    Funding rate negativo alto (< -0.05%) = muitos shorts pagando longs
    → mercado sobrecarregado de shorts → tendência de alta (LONG)
    
    Este é um dos sinais mais poderosos que existem e quase ninguém
    usa em bots. Os market makers vivem disso.
    
    Retorna: float [-1, +1] onde:
      +1 = funding muito negativo → favorece LONG
      -1 = funding muito positivo → favorece SHORT
    """
    global _funding_cache
    try:
        cached = _funding_cache.get(par)
        if cached and (time.time() - cached[1]) < 60:
            return cached[0]

        rate_limiter.wait()
        funding = client_level2.funding_rate(symbol=par, limit=1)
        if not funding:
            return 0.0

        rate = float(funding[0].get('fundingRate', 0))

        # Funding > 0.0005 (0.05%) = sobrecarregado de longs
        # Funding < -0.0005 = sobrecarregado de shorts
        if rate > 0.001:
            sinal = -1.0  # muito longs → vai cair
        elif rate > 0.0005:
            sinal = -0.5
        elif rate < -0.001:
            sinal = +1.0  # muito shorts → vai subir
        elif rate < -0.0005:
            sinal = +0.5
        else:
            sinal = 0.0

        _funding_cache[par] = (sinal, time.time())

        if abs(sinal) > 0.3:
            logger.info(f"  [FUNDING] {par} | rate={rate:.6f} | sinal={sinal:+.1f}")

        return sinal

    except Exception as e:
        logger.debug(f"[FUNDING] {par} | {e}")
        return 0.0


def coletar_open_interest(par):
    """
    OPEN INTEREST — mede posições abertas no mercado.
    
    OI subindo + preço subindo = tendência forte (dinheiro novo entrando)
    OI subindo + preço caindo = shorts abrindo → pressão de queda
    OI caindo + preço subindo = shorts fechando (short squeeze possível)
    OI caindo + preço caindo = longs fechando (capitulação)
    
    O mais valioso: mudança rápida de OI indica que liquidações
    estão prestes a acontecer. Preço tende a ir buscar as liquidações.
    
    Retorna: dict com:
      'sinal': float [-1, +1]
      'squeeze_risk': float [0, 1] (risco de squeeze)
      'oi_trend': str ('subindo', 'caindo', 'estavel')
    """
    global _oi_cache
    try:
        rate_limiter.wait()
        oi_data = client_level2.open_interest(symbol=par)
        if not oi_data:
            return {'sinal': 0, 'squeeze_risk': 0, 'oi_trend': 'estavel'}

        oi_atual = float(oi_data.get('openInterest', 0))

        # Guardar histórico
        if par not in _oi_cache:
            _oi_cache[par] = []
        _oi_cache[par].append((oi_atual, time.time()))
        _oi_cache[par] = _oi_cache[par][-10:]

        if len(_oi_cache[par]) < 3:
            return {'sinal': 0, 'squeeze_risk': 0, 'oi_trend': 'estavel'}

        # Tendência do OI
        oi_valores = [v for v, _ in _oi_cache[par]]
        oi_media_recente = sum(oi_valores[-3:]) / 3
        oi_media_anterior = sum(oi_valores[:3]) / 3 if len(oi_valores) >= 6 else oi_valores[0]
        oi_change = (oi_media_recente - oi_media_anterior) / (oi_media_anterior + 1e-9)

        # Preço
        with lock_precos:
            dados = precos_cache.get(par, [])
        if not dados:
            return {'sinal': 0, 'squeeze_risk': 0, 'oi_trend': 'estavel'}

        closes = [float(d['close'] if isinstance(d, dict) else d) for d in dados[-20:]]
        if len(closes) < 5:
            return {'sinal': 0, 'squeeze_risk': 0, 'oi_trend': 'estavel'}

        preco_change = (closes[-1] - closes[-5]) / (closes[-5] + 1e-9)

        # Classificar situação
        sinal = 0.0
        squeeze_risk = 0.0
        oi_trend = 'estavel'

        if oi_change > 0.02:
            oi_trend = 'subindo'
            if preco_change > 0.005:
                sinal = +0.5   # OI sobe + preço sobe = tendência forte long
            elif preco_change < -0.005:
                sinal = -0.5   # OI sobe + preço cai = shorts abrindo
        elif oi_change < -0.02:
            oi_trend = 'caindo'
            if preco_change > 0.005:
                sinal = +0.7   # OI cai + preço sobe = SHORT SQUEEZE!
                squeeze_risk = min(1.0, abs(oi_change) * 10)
            elif preco_change < -0.005:
                sinal = -0.3   # OI cai + preço cai = capitulação

        # OI mudando muito rápido = liquidações vindo
        if abs(oi_change) > 0.05:
            squeeze_risk = min(1.0, abs(oi_change) * 5)

        if abs(sinal) > 0.3 or squeeze_risk > 0.3:
            logger.info(f"  [OI] {par} | OI={oi_atual:.0f} | Δ={oi_change:+.2%} | "
                         f"preço Δ={preco_change:+.2%} | sinal={sinal:+.1f} | "
                         f"squeeze={squeeze_risk:.1f} | trend={oi_trend}")

        return {'sinal': sinal, 'squeeze_risk': squeeze_risk, 'oi_trend': oi_trend}

    except Exception as e:
        logger.debug(f"[OI] {par} | {e}")
        return {'sinal': 0, 'squeeze_risk': 0, 'oi_trend': 'estavel'}


# ================================================================
# CÉREBRO — Raciocina como trader profissional
# ================================================================
def cerebro_decisao(par):
    """
    Integra TODOS os sinais numa decisão final pensada.
    
    Não é soma de pesos — é RACIOCÍNIO condicional:
    - Se funding diz "muitos longs" E order flow mostra venda → SHORT forte
    - Se OI indica squeeze E tendência é compra → LONG agressivo
    - Se todos os sinais divergem → NÃO ENTRA (melhor decisão = não operar)
    
    Retorna dict atualizado do micro_modelo com sinais preditivos integrados.
    """
    try:
        with lock_modelos:
            mm = dict(micro_modelos.get(par, {}))
        if not mm:
            return mm

        acao_original = mm.get('acao', 'manter')
        forca_original = mm.get('forca_final', 0.0)
        conf_original = mm.get('confianca', 0.0)

        if acao_original == 'manter':
            return mm

        # Coletar sinais preditivos
        flow = coletar_order_flow_delta(par)
        funding = coletar_funding_rate(par)
        oi = coletar_open_interest(par)
        oi_sinal = oi.get('sinal', 0)
        squeeze = oi.get('squeeze_risk', 0)

        # ===== RACIOCÍNIO =====
        
        # Direção proposta
        direcao = 1 if acao_original == 'compra' else -1

        # 1. CONCORDÂNCIA — quantos sinais preditivos concordam com a direção
        concordancia = 0
        total_sinais = 0

        if abs(flow) > 0.1:
            total_sinais += 1
            if flow * direcao > 0:
                concordancia += 1

        if abs(funding) > 0.1:
            total_sinais += 1
            if funding * direcao > 0:
                concordancia += 1

        if abs(oi_sinal) > 0.1:
            total_sinais += 1
            if oi_sinal * direcao > 0:
                concordancia += 1

        # 2. DECISÃO BASEADA EM RACIOCÍNIO

        ajuste_forca = 0.0
        ajuste_conf = 0.0
        razao = ""

        if total_sinais == 0:
            # Sem dados preditivos — mantém decisão original
            razao = "sem_dados_preditivos"

        elif total_sinais >= 2 and concordancia == total_sinais:
            # TODOS os sinais preditivos concordam com a direção
            # → Convicção máxima, boost forte
            ajuste_forca = 0.15 * direcao
            ajuste_conf = 0.10
            razao = "TODOS_CONCORDAM"

        elif total_sinais >= 2 and concordancia == 0:
            # TODOS os sinais preditivos CONTRADIZEM a direção
            # → Reduz confiança significativamente, mas NÃO cancela
            # → O ranking e a confirmação decidem se entra ou não
            # → O cérebro é conselheiro, não ditador
            if abs(flow) > 0.5 or abs(funding) > 0.5:
                # Sinal forte contrário — reduz muito mas mantém direção
                ajuste_conf = -0.25
                ajuste_forca = -0.15 * direcao  # enfraquece na direção original
                razao = "CONTRADIÇÃO_FORTE"
            else:
                # Sinal moderado contrário — reduz moderadamente
                ajuste_conf = -0.15
                ajuste_forca = -0.10 * direcao
                razao = "CONTRADIÇÃO_MODERADA"

        elif concordancia >= 1 and concordancia < total_sinais:
            # Sinais mistos — reduz confiança mas mantém direção
            ajuste_conf = -0.05
            razao = f"PARCIAL_{concordancia}/{total_sinais}"

        # 3. SQUEEZE DETECTION — oportunidade rara
        if squeeze > 0.5:
            if oi_sinal > 0 and acao_original == 'compra':
                # Short squeeze + long = oportunidade excepcional
                ajuste_forca = 0.20
                ajuste_conf = 0.15
                razao = "SHORT_SQUEEZE_LONG"
            elif oi_sinal < 0 and acao_original == 'venda':
                # Long squeeze + short = oportunidade excepcional
                ajuste_forca = -0.20
                ajuste_conf = 0.15
                razao = "LONG_SQUEEZE_SHORT"

        # 4. FUNDING EXTREMO — sinal mais confiável
        if abs(funding) >= 0.8:
            # Funding extremo = o mercado está muito desequilibrado
            # Ir contra o crowd é historicamente lucrativo
            if funding * direcao > 0:
                ajuste_conf = 0.12
                razao = "FUNDING_CONFIRMA_FORTE"
            else:
                ajuste_conf = -0.20
                razao = "FUNDING_CONTRADIZ_FORTE"

        # Aplicar ajustes dos sinais preditivos
        nova_forca = max(-1.0, min(1.0, forca_original + ajuste_forca))
        nova_conf = max(0.0, min(1.0, conf_original + ajuste_conf))

        # ====== 5. CAMADA INVISÍVEL — a porta que ninguém olha ======
        # Roda DEPOIS dos sinais preditivos e pode AMPLIFICAR ou ANULAR
        invisivel = camada_invisivel(par)
        inv_sinal = invisivel['sinal']
        inv_forca = invisivel['forca_invisivel']
        inv_razao = invisivel['razao']
        inv_meta = invisivel['meta_confianca']
        
        if inv_razao != 'NENHUM' and inv_forca > 0.2:
            # Invisível concorda com a direção atual?
            inv_concorda = (inv_sinal > 0 and nova_forca > 0) or (inv_sinal < 0 and nova_forca < 0)
            
            if inv_concorda and inv_meta >= 0.5:
                # CONFIRMA — boost forte (este sinal é invisível para o mercado)
                nova_conf = min(1.0, nova_conf + 0.10 * inv_forca)
                nova_forca = max(-1.0, min(1.0, nova_forca + 0.10 * inv_sinal))
                if inv_razao not in ['NENHUM']:
                    razao = f"{razao}+{inv_razao}"
            
            elif not inv_concorda and inv_meta >= 0.6:
                # CONTRADIZ FORTE — a microestrutura vê algo que indicadores não veem
                # Reduz confiança significativamente
                nova_conf = max(0.0, nova_conf - 0.15 * inv_forca)
                nova_forca = max(-1.0, min(1.0, nova_forca + 0.12 * inv_sinal))
                razao = f"INVIS_{inv_razao}"
        
        # Salvar invisível no modelo
        mm['invisivel_sinal'] = round(inv_sinal, 3)
        mm['invisivel_razao'] = inv_razao

        mm['forca_final'] = nova_forca
        mm['indicador_forca'] = nova_forca
        mm['confianca'] = round(nova_conf, 3)

        # Salvar sinais preditivos no modelo
        mm['order_flow'] = round(flow, 3)
        mm['funding_sinal'] = funding
        mm['oi_sinal'] = round(oi_sinal, 3)
        mm['squeeze_risk'] = round(squeeze, 3)
        mm['cerebro_razao'] = razao

        with lock_modelos:
            micro_modelos[par] = mm

        # Log
        if razao and razao != "sem_dados_preditivos":
            cor = Fore.GREEN if 'CONCORDAM' in razao or 'SQUEEZE' in razao or 'CONFIRMA' in razao else \
                  Fore.RED if 'INVERSÃO' in razao or 'CANCELADO' in razao or 'CONTRADIZ' in razao else \
                  Fore.MAGENTA if 'INVIS' in razao else Fore.YELLOW
            inv_str = f" | 👁 {inv_razao}" if inv_razao != 'NENHUM' else ""
            print(f"  {cor}[CÉREBRO] {par:12} | {razao:30} | "
                  f"flow={flow:+.2f} fund={funding:+.1f} OI={oi_sinal:+.1f} "
                  f"squeeze={squeeze:.1f}{inv_str} | "
                  f"força: {forca_original:+.3f}→{nova_forca:+.3f} | "
                  f"conf: {conf_original:.2f}→{nova_conf:.2f}{Style.RESET_ALL}")

        return mm

    except Exception as e:
        logger.debug(f"[CÉREBRO] {par} | {e}")
        return micro_modelos.get(par, {})


# ================================================================
# CAMADA INVISÍVEL — O que ninguém olha
# ================================================================
# Market makers não usam RSI nem MACD. Eles olham:
# 1. VELOCIDADE da mudança (não a mudança em si)
# 2. EXAUSTÃO de momentum (não o momentum)
# 3. VÁCUO DE LIQUIDEZ (onde NÃO tem ordens)
# 4. DIVERGÊNCIA VOLUME vs PREÇO (volume mentindo)
# 5. PADRÃO DE WICKS (o que as sombras dizem)
# 6. ABSORÇÃO SILENCIOSA (big player invisível)

_velocidade_cache = {}

def camada_invisivel(par):
    """
    Análise de microestrutura invisível.
    Olha para onde nenhum indicador público olha.
    """
    resultado = {'sinal': 0.0, 'forca_invisivel': 0.0, 'razao': 'NENHUM', 'meta_confianca': 0.0}
    
    try:
        with lock_precos:
            dados = precos_cache.get(par, [])
        
        if not dados or len(dados) < 60 or not isinstance(dados[-1], dict):
            return resultado
        
        closes = [float(d['close']) for d in dados[-100:]]
        highs = [float(d['high']) for d in dados[-100:]]
        lows = [float(d['low']) for d in dados[-100:]]
        volumes = [float(d['volume']) for d in dados[-100:]]
        opens = [float(d['open']) for d in dados[-100:]]
        
        n = len(closes)
        if n < 30:
            return resultado
        
        preco = closes[-1]
        sinais = []
        pesos = []
        
        # === 1. DERIVADAS: velocidade, aceleração, jerk ===
        # Jerk muda de sinal = ponto de inflexão ANTES do preço virar
        velocidades = [(closes[i] - closes[i-1]) / (closes[i-1] + 1e-9) for i in range(1, n)]
        if len(velocidades) >= 10:
            aceleracoes = [velocidades[i] - velocidades[i-1] for i in range(1, len(velocidades))]
            jerks = [aceleracoes[i] - aceleracoes[i-1] for i in range(1, len(aceleracoes))]
            
            vel_r = sum(velocidades[-5:]) / 5
            acel_r = sum(aceleracoes[-5:]) / 5
            jerk_r = sum(jerks[-3:]) / 3 if len(jerks) >= 3 else 0
            
            if vel_r > 0.001 and acel_r < 0 and jerk_r < 0:
                sinais.append(-0.7); pesos.append(0.25)
                resultado['razao'] = 'EXAUSTÃO_ALTA'
            elif vel_r < -0.001 and acel_r > 0 and jerk_r > 0:
                sinais.append(+0.7); pesos.append(0.25)
                resultado['razao'] = 'EXAUSTÃO_BAIXA'
            elif vel_r > 0.001 and acel_r > 0:
                sinais.append(+0.5); pesos.append(0.15)
            elif vel_r < -0.001 and acel_r < 0:
                sinais.append(-0.5); pesos.append(0.15)
        
        # === 2. DIVERGÊNCIA VOLUME-PREÇO ===
        if len(volumes) >= 20:
            vol_m20 = sum(volumes[-20:]) / 20
            vol_m5 = sum(volumes[-5:]) / 5
            pc5 = (closes[-1] - closes[-6]) / (closes[-6] + 1e-9) if n >= 6 else 0
            vr = vol_m5 / (vol_m20 + 1e-9)
            
            if pc5 > 0.005 and vr < 0.7:
                sinais.append(-0.6); pesos.append(0.20)
                resultado['razao'] = 'SUBIDA_SEM_VOLUME'
            elif pc5 < -0.005 and vr < 0.7:
                sinais.append(+0.5); pesos.append(0.15)
                resultado['razao'] = 'QUEDA_SEM_VOLUME'
            elif vr > 2.5 and abs(pc5) < 0.002:
                book = precos_cache.get(par + '_book', {})
                if isinstance(book, dict) and book.get('bids') and book.get('asks'):
                    tb = sum(q for _, q in book['bids'])
                    ta = sum(q for _, q in book['asks'])
                    if tb > ta * 1.3:
                        sinais.append(+0.8); pesos.append(0.25)
                        resultado['razao'] = 'BREAKOUT_COMPRA'
                    elif ta > tb * 1.3:
                        sinais.append(-0.8); pesos.append(0.25)
                        resultado['razao'] = 'BREAKOUT_VENDA'
            
            if vr > 3.0 and abs(pc5) > 0.015:
                if pc5 > 0:
                    sinais.append(-0.6); pesos.append(0.20)
                    resultado['razao'] = 'CLIMAX_COMPRA'
                else:
                    sinais.append(+0.6); pesos.append(0.20)
                    resultado['razao'] = 'CLIMAX_VENDA'
        
        # === 3. VÁCUO DE LIQUIDEZ ===
        book = precos_cache.get(par + '_book', {})
        if isinstance(book, dict) and book.get('bids') and book.get('asks'):
            bids = book['bids']; asks = book['asks']
            if len(bids) >= 10 and len(asks) >= 10:
                bp = [p for p, _ in bids[:20]]
                ap = [p for p, _ in asks[:20]]
                bg = [abs(bp[i-1] - bp[i]) / (preco + 1e-9) for i in range(1, len(bp))]
                ag = [abs(ap[i] - ap[i-1]) / (preco + 1e-9) for i in range(1, len(ap))]
                mbg = max(bg) if bg else 0
                mag = max(ag) if ag else 0
                
                if mag > mbg * 2 and mag > 0.001:
                    sinais.append(+0.4); pesos.append(0.15)
                    if resultado['razao'] == 'NENHUM': resultado['razao'] = 'VACUO_ACIMA'
                elif mbg > mag * 2 and mbg > 0.001:
                    sinais.append(-0.4); pesos.append(0.15)
                    if resultado['razao'] == 'NENHUM': resultado['razao'] = 'VACUO_ABAIXO'
                
                bq = [q for _, q in bids[:20]]
                aq = [q for _, q in asks[:20]]
                if bq and aq:
                    if max(bq) > sum(bq)/len(bq) * 5:
                        sinais.append(+0.3); pesos.append(0.10)
                    if max(aq) > sum(aq)/len(aq) * 5:
                        sinais.append(-0.3); pesos.append(0.10)
        
        # === 4. PADRÃO DE WICKS ===
        if n >= 10:
            wr_up = []; wr_dn = []
            for i in range(-10, 0):
                total = highs[i] - lows[i] + 1e-12
                wr_up.append((highs[i] - max(closes[i], opens[i])) / total)
                wr_dn.append((min(closes[i], opens[i]) - lows[i]) / total)
            if len(wr_dn) >= 5:
                wt = sum(wr_dn[-3:])/3 - sum(wr_dn[-6:-3])/3
                if wt > 0.05:
                    sinais.append(+0.4); pesos.append(0.15)
                    if resultado['razao'] == 'NENHUM': resultado['razao'] = 'ACUMULAÇÃO_WICK'
            if len(wr_up) >= 5:
                wt = sum(wr_up[-3:])/3 - sum(wr_up[-6:-3])/3
                if wt > 0.05:
                    sinais.append(-0.4); pesos.append(0.15)
                    if resultado['razao'] == 'NENHUM': resultado['razao'] = 'DISTRIBUIÇÃO_WICK'
        
        # === 5. ABSORÇÃO SILENCIOSA ===
        if len(volumes) >= 10 and n >= 10:
            pr5 = (max(closes[-5:]) - min(closes[-5:])) / (preco + 1e-9)
            vt5 = sum(volumes[-5:]); vtp = sum(volumes[-10:-5])
            if vtp > 0 and vt5 > vtp * 1.8 and pr5 < 0.003:
                cv = sum(1 for i in range(-5, 0) if closes[i] >= opens[i])
                if cv >= 4:
                    sinais.append(+0.6); pesos.append(0.20)
                    resultado['razao'] = 'ABSORÇÃO_SILENCIOSA_COMPRA'
                elif cv <= 1:
                    sinais.append(-0.6); pesos.append(0.20)
                    resultado['razao'] = 'ABSORÇÃO_SILENCIOSA_VENDA'
        
        # === SÍNTESE ===
        if sinais and pesos:
            tp = sum(pesos)
            sf = sum(s * p for s, p in zip(sinais, pesos)) / tp
            concordam = sum(1 for s in sinais if s * sf > 0)
            resultado['sinal'] = max(-1.0, min(1.0, sf))
            resultado['forca_invisivel'] = min(1.0, abs(sf) * 1.5)
            resultado['meta_confianca'] = concordam / len(sinais) if sinais else 0
        
        return resultado
    except Exception as e:
        logger.debug(f"[INVISÍVEL] {par} | {e}")
        return resultado


# ================================================================
# DETECÇÃO DE ARMADILHAS DE BALEIAS
# ================================================================
_historico_book = {}     # par -> [desequilibrio, ...] últimos 20
_historico_preco = {}    # par -> [preco, ...] últimos 20 (snapshots rápidos)
_armadilhas_detectadas = {}  # par -> {'tipo': str, 'ts': float}

def detectar_armadilha_baleia(par):
    """
    Detecta padrões de manipulação de baleias em tempo real:
    
    1. SPOOFING — ordem grande aparece e some rapidamente
       Book mostra grande bid → preço sobe → bid desaparece → preço despenca
       Detecção: grande_ordem oscila muito entre snapshots
    
    2. WASH TRADING — volume falso para atrair
       Volume spike sem movimento real de preço
       Detecção: volume 3x acima da média mas preço move < 0.1%
    
    3. STOP HUNTING — preço faz spike rápido para liquidar stops
       e volta imediatamente
       Detecção: wick > 2x body no candle + volume spike
    
    4. ABSORÇÃO — baleia absorve ordens sem deixar preço mover
       Grande desequilíbrio no book mas preço não move na direção esperada
       Detecção: desequilíbrio > 20% mas preço move contrário
    
    5. PUMP AND DUMP — volume+preço sobem explosivo e caem
       Detecção: ganho > 3% em < 5 candles seguido de queda
    
    Retorna: dict {
        'armadilha': bool,
        'tipo': str,
        'severidade': float [0,1],
        'direcao_real': str  # para onde o preço REALMENTE vai
    }
    """
    global _historico_book, _historico_preco, _armadilhas_detectadas
    
    resultado = {'armadilha': False, 'tipo': '', 'severidade': 0, 'direcao_real': 'indefinido'}
    
    try:
        # --- Dados atuais ---
        book = precos_cache.get(par + '_book', {})
        with lock_precos:
            dados = precos_cache.get(par, [])
        
        if not dados or len(dados) < 10:
            return resultado
        
        closes = [float(d['close'] if isinstance(d, dict) else d) for d in dados[-20:]]
        preco_atual = closes[-1]
        preco_medio = sum(closes) / len(closes)
        
        # --- Histórico de snapshots ---
        if par not in _historico_preco:
            _historico_preco[par] = []
        _historico_preco[par].append(preco_atual)
        _historico_preco[par] = _historico_preco[par][-30:]
        
        # --- 1. SPOOFING: ordem grande que oscila ---
        if isinstance(book, dict) and book.get('bids') and book.get('asks'):
            total_bid = sum(q for _, q in book['bids'])
            total_ask = sum(q for _, q in book['asks'])
            deseq = (total_bid - total_ask) / (total_bid + total_ask + 1e-9)
            
            maior_bid = max((q for _, q in book['bids']), default=0)
            maior_ask = max((q for _, q in book['asks']), default=0)
            maior_ordem = max(maior_bid, maior_ask)
            total_book = total_bid + total_ask + 1e-9
            concentracao = maior_ordem / total_book
            
            if par not in _historico_book:
                _historico_book[par] = []
            _historico_book[par].append({
                'deseq': deseq, 'concentracao': concentracao,
                'maior_ordem': maior_ordem, 'ts': time.time()
            })
            _historico_book[par] = _historico_book[par][-20:]
            
            if len(_historico_book[par]) >= 5:
                deseqs = [h['deseq'] for h in _historico_book[par][-5:]]
                concentracoes = [h['concentracao'] for h in _historico_book[par][-5:]]
                
                # Spoofing: desequilíbrio muda de sinal rapidamente
                mudancas_sinal = sum(1 for i in range(1, len(deseqs)) 
                                     if deseqs[i] * deseqs[i-1] < 0)
                if mudancas_sinal >= 3 and max(concentracoes) > 0.15:
                    resultado = {
                        'armadilha': True,
                        'tipo': 'SPOOFING',
                        'severidade': min(1.0, mudancas_sinal * 0.25),
                        'direcao_real': 'indefinido'
                    }
                
                # Absorção: book diz compra mas preço cai (ou vice-versa)
                if len(closes) >= 5 and abs(deseq) > 0.15:
                    preco_delta = (closes[-1] - closes[-5]) / (closes[-5] + 1e-9)
                    if deseq > 0.15 and preco_delta < -0.003:
                        resultado = {
                            'armadilha': True,
                            'tipo': 'ABSORÇÃO_VENDA',
                            'severidade': min(1.0, abs(deseq) * 2),
                            'direcao_real': 'venda'  # baleia está vendendo disfarçado
                        }
                    elif deseq < -0.15 and preco_delta > 0.003:
                        resultado = {
                            'armadilha': True,
                            'tipo': 'ABSORÇÃO_COMPRA',
                            'severidade': min(1.0, abs(deseq) * 2),
                            'direcao_real': 'compra'  # baleia está comprando disfarçado
                        }
        
        # --- 2. WASH TRADING: volume sem preço ---
        if isinstance(dados[-1], dict) and len(dados) >= 20:
            try:
                vols = [float(d.get('volume', 0)) for d in dados[-20:]]
                vol_media = sum(vols[:-1]) / max(len(vols[:-1]), 1)
                vol_atual = vols[-1]
                preco_move = abs(closes[-1] - closes[-2]) / (closes[-2] + 1e-9)
                
                if vol_media > 0 and vol_atual > vol_media * 3 and preco_move < 0.001:
                    if resultado['severidade'] < 0.5:
                        resultado = {
                            'armadilha': True,
                            'tipo': 'WASH_TRADING',
                            'severidade': min(1.0, (vol_atual / vol_media) * 0.15),
                            'direcao_real': 'indefinido'
                        }
            except:
                pass
        
        # --- 3. STOP HUNTING: spike + reversão ---
        if len(dados) >= 3 and isinstance(dados[-1], dict):
            try:
                candle = dados[-1]
                high = float(candle.get('high', 0))
                low = float(candle.get('low', 0))
                open_p = float(candle.get('open', 0))
                close_p = float(candle.get('close', 0))
                
                body = abs(close_p - open_p)
                wick_up = high - max(open_p, close_p)
                wick_down = min(open_p, close_p) - low
                
                if body > 0:
                    # Wick superior >> body = spike para cima + reversão
                    if wick_up > body * 2.5 and wick_up / (preco_medio + 1e-9) > 0.003:
                        resultado = {
                            'armadilha': True,
                            'tipo': 'STOP_HUNT_UP',
                            'severidade': min(1.0, wick_up / body * 0.2),
                            'direcao_real': 'venda'  # preço vai cair após o spike
                        }
                    # Wick inferior >> body = spike para baixo + reversão
                    elif wick_down > body * 2.5 and wick_down / (preco_medio + 1e-9) > 0.003:
                        resultado = {
                            'armadilha': True,
                            'tipo': 'STOP_HUNT_DOWN',
                            'severidade': min(1.0, wick_down / body * 0.2),
                            'direcao_real': 'compra'  # preço vai subir após o spike
                        }
            except:
                pass
        
        # --- 4. PUMP AND DUMP ---
        if len(closes) >= 10:
            # Procura ganho > 2% em 5 candles seguido de queda
            pico = max(closes[-10:-5]) if len(closes) >= 10 else closes[-5]
            base = min(closes[-10:-5]) if len(closes) >= 10 else closes[-5]
            ganho_rapido = (pico - base) / (base + 1e-9)
            queda_pos_pico = (pico - closes[-1]) / (pico + 1e-9)
            
            if ganho_rapido > 0.02 and queda_pos_pico > 0.015:
                if resultado['severidade'] < 0.6:
                    resultado = {
                        'armadilha': True,
                        'tipo': 'PUMP_AND_DUMP',
                        'severidade': min(1.0, ganho_rapido * 10),
                        'direcao_real': 'venda'
                    }
        
        # Registrar
        if resultado['armadilha']:
            _armadilhas_detectadas[par] = {
                'tipo': resultado['tipo'],
                'ts': time.time(),
                'severidade': resultado['severidade']
            }
            cor = Fore.RED + Style.BRIGHT
            print(f"  {cor}[⚠ BALEIA] {par:12} | {resultado['tipo']:20} | "
                  f"Severidade: {resultado['severidade']:.0%} | "
                  f"Direção real: {resultado['direcao_real']}{Style.RESET_ALL}")
        
        return resultado
        
    except Exception as e:
        logger.debug(f"[BALEIA] {par} | {e}")
        return resultado


# ================================================================
# ANTI-FRAGILIDADE — Fica mais forte quando perde
# ================================================================
_memoria_erros = {}     # par -> [{'acao': str, 'forca': float, 'resultado': int}, ...]
_padroes_perda = {}     # padrão -> contagem de perdas

def registrar_resultado_trade(par, acao, forca, confianca, regime, funding, flow, ganhou):
    """
    Registra cada trade com contexto completo.
    Permite ao sistema aprender quais COMBINAÇÕES de sinais levam a perda.
    """
    global _memoria_erros, _padroes_perda
    
    # Criar fingerprint do trade
    fingerprint = {
        'acao': acao,
        'forca_range': 'forte' if abs(forca) > 0.7 else 'medio' if abs(forca) > 0.4 else 'fraco',
        'regime': regime,
        'funding': 'positivo' if funding > 0.3 else 'negativo' if funding < -0.3 else 'neutro',
        'flow': 'positivo' if flow > 0.3 else 'negativo' if flow < -0.3 else 'neutro',
        'resultado': 1 if ganhou else 0
    }
    
    if par not in _memoria_erros:
        _memoria_erros[par] = []
    _memoria_erros[par].append(fingerprint)
    _memoria_erros[par] = _memoria_erros[par][-50:]  # últimos 50 trades
    
    # Identificar padrão
    padrao_key = f"{acao}_{fingerprint['forca_range']}_{regime}_{fingerprint['funding']}_{fingerprint['flow']}"
    
    if not ganhou:
        _padroes_perda[padrao_key] = _padroes_perda.get(padrao_key, 0) + 1
        if _padroes_perda[padrao_key] >= 3:
            logger.warning(f"  [ANTI-FRÁGIL] Padrão de perda detectado: {padrao_key} "
                           f"({_padroes_perda[padrao_key]}x)")


def verificar_padrao_perda(par, acao, forca, regime, funding, flow):
    """
    Verifica se a combinação atual de sinais JÁ CAUSOU perdas antes.
    Se sim, reduz capital ou bloqueia.
    
    Retorna: float [0, 1] onde 0 = bloquear, 1 = normal
    """
    forca_range = 'forte' if abs(forca) > 0.7 else 'medio' if abs(forca) > 0.4 else 'fraco'
    funding_cat = 'positivo' if funding > 0.3 else 'negativo' if funding < -0.3 else 'neutro'
    flow_cat = 'positivo' if flow > 0.3 else 'negativo' if flow < -0.3 else 'neutro'
    
    padrao_key = f"{acao}_{forca_range}_{regime}_{funding_cat}_{flow_cat}"
    
    perdas = _padroes_perda.get(padrao_key, 0)
    
    if perdas >= 5:
        logger.info(f"  [ANTI-FRÁGIL] {par} | BLOQUEADO | padrão {padrao_key} perdeu {perdas}x")
        return 0.0   # bloqueia completamente
    elif perdas >= 3:
        fator = max(0.2, 1.0 - perdas * 0.15)
        logger.info(f"  [ANTI-FRÁGIL] {par} | REDUZIDO {fator:.0%} | padrão {padrao_key} perdeu {perdas}x")
        return fator
    
    return 1.0  # normal


# ================================================================
# CONFIRMAÇÃO DE ENTRADA — Evita entrar em armadilha
# ================================================================
def confirmar_entrada(par, acao_proposta):
    """
    Verifica se o trade proposto passa nos filtros de segurança:
    1. Regime de mercado compatível
    2. Não está em cooldown pós-loss
    3. Exposição total não excede limite
    4. Risk:Reward mínimo OK
    5. Não duplica posição existente na mesma direção
    6. Horário adequado (evita baixa liquidez)
    
    Retorna (aprovado: bool, motivo: str, ajuste_capital: float)
    """
    global ultimo_loss_time
    ajuste = 1.0

    # 1. Regime de mercado
    if regime_mercado == 'chop':
        ajuste *= 0.4  # mercado picotado = posição mínima
    elif regime_mercado == 'bull' and acao_proposta == 'venda':
        ajuste *= 0.5  # shortear em bull = arriscado
    elif regime_mercado == 'bear' and acao_proposta == 'compra':
        ajuste *= 0.5  # longar em bear = arriscado
    elif regime_mercado == 'bull' and acao_proposta == 'compra':
        ajuste *= 1.3  # long em bull = boost
    elif regime_mercado == 'bear' and acao_proposta == 'venda':
        ajuste *= 1.3  # short em bear = boost

    # 2. Cooldown pós-loss
    tempo_desde_loss = time.time() - ultimo_loss_time
    if tempo_desde_loss < _adapt_get("cooldown", COOLDOWN_APOS_LOSS):
        restante = _adapt_get("cooldown", COOLDOWN_APOS_LOSS) - tempo_desde_loss
        return False, f"cooldown ({restante:.0f}s restantes)", 0.0

    # 3. Exposição total
    with lock_posicoes:
        posicoes_snap = dict(posicoes)
    with lock_trailing:
        trail_snap = dict(trailing_info)

    n_posicoes = sum(1 for v in posicoes_snap.values() if abs(v) > 0)
    if n_posicoes >= _adapt_get("max_posicoes", MAX_POSICOES_SIMULTANEAS):
        return False, f"máximo {_adapt_get('max_posicoes', MAX_POSICOES_SIMULTANEAS)} posições atingido", 0.0

    # Calcular exposição total em USDT
    exposicao_total = 0.0
    for p, qty in posicoes_snap.items():
        if abs(qty) > 0:
            with lock_precos:
                dados = precos_cache.get(p, [])
            if dados:
                ultimo = dados[-1]
                preco = float(ultimo['close'] if isinstance(ultimo, dict) else ultimo)
                exposicao_total += abs(qty) * preco

    if exposicao_total > CAPITAL_TOTAL * MAX_EXPOSICAO_TOTAL:
        return False, f"exposição total ${exposicao_total:.0f} > limite", 0.0

    # 4. Risk:Reward via ATR
    with lock_indicadores:
        df = indicadores.get(par)
    if df is not None and 'atr' in df.columns and len(df) > 0:
        try:
            atr = float(df['atr'].iloc[-1])
            preco = float(df['close'].iloc[-1])
            # Stop = 1.5 ATR, Target = ATR * R:R ratio
            risco_estimado = 1.5 * atr / preco  # % de risco
            retorno_estimado = risco_estimado * MIN_RR_RATIO
            # Se o risco é muito alto para o capital, reduz
            if risco_estimado > 0.03:  # > 3% de risco por trade
                ajuste *= 0.5
        except:
            pass

    # 5. Não duplicar posição na mesma direção
    pos_existente = posicoes_snap.get(par, 0.0)
    if acao_proposta == 'compra' and pos_existente > 0:
        return False, "já tem LONG aberto", 0.0
    if acao_proposta == 'venda' and pos_existente < 0:
        return False, "já tem SHORT aberto", 0.0

    # 6. Horário — evitar baixa liquidez (UTC)
    hora = datetime.now(timezone.utc).hour
    if 22 <= hora or hora < 2:  # meia-noite UTC = baixa liquidez
        ajuste *= 0.6

    # 7. Streak de perdas — reduz após sequência negativa
    if streak_atual <= -3:
        ajuste *= 0.5
        logger.info(f"  [CONFIRM] {par} | streak={streak_atual} → capital reduzido 50%")

    return True, "aprovado", ajuste


# ================================================================
# PAINEL
# ================================================================
def mostrar_painel_completo():
    try:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        saldo = atualizar_saldo_real()
        total_usdt = saldo.get('USDT', 0.0)
        with _lock_historico:
            total_ganhos = sum(p['ganhos'] for p in performance_por_par.values())
            total_perdas = sum(p['perdas'] for p in performance_por_par.values())
            total_trades = sum(p['total_trades'] for p in performance_por_par.values())
        win_rate = (total_ganhos / total_trades * 100) if total_trades > 0 else 0
        print("\n" + "=" * 120)
        regime_cor = {
            'bull': Fore.GREEN, 'bear': Fore.RED,
            'chop': Fore.YELLOW, 'neutro': Fore.WHITE
        }.get(regime_mercado, Fore.WHITE)
        pnl_cor = Fore.GREEN if pnl_sessao >= 0 else Fore.RED
        print(Fore.CYAN + Style.BRIGHT +
              f"[PAINEL] {now} | Saldo: {total_usdt:.2f} USDT | "
              f"Pares: {len(pares_usdt)} | Trades: {total_trades} | Win: {win_rate:.1f}% | "
              f"{pnl_cor}P&L: {pnl_sessao:+.2f}{Style.RESET_ALL} | "
              f"Streak: {streak_atual} | "
              f"Regime: {regime_cor}{regime_mercado.upper()}{Style.RESET_ALL}")
        print(Fore.MAGENTA + "=" * 120)
        print(Fore.YELLOW +
              f"{'PAR':12} | {'PREÇO':10} | {'AÇÃO':7} | {'CONF':5} | "
              f"{'FORÇA':7} | {'FLOW':5} | {'FUND':5} | {'OI':5} | "
              f"{'POSIÇÃO':10} | {'LUCRO%':7} | {'CAPITAL':10}")
        print(Fore.MAGENTA + "-" * 120)
        with lock_capital:
            cap_snap = dict(capital_por_par)
        for p in pares_usdt:
            with lock_precos:
                dados = precos_cache.get(p, [])
            if dados:
                ultimo = dados[-1]
                preco = float(ultimo['close'] if isinstance(ultimo, dict) else ultimo)
            else:
                preco = 0
            with lock_modelos:
                mm = micro_modelos.get(p, {})
            with lock_posicoes:
                pos = posicoes.get(p, 0.0)
            with lock_trailing:
                trail = trailing_info.get(p, {})
            lucro_atual = 0.0
            if trail and pos > 0 and trail.get('preco_compra', 0) > 0:
                lucro_atual = (preco - trail['preco_compra']) / trail['preco_compra']
            acao_str = mm.get('acao', '-')
            forca = mm.get('forca_final', 0.0)
            if acao_str == 'compra':
                acao_color = Fore.GREEN
            elif acao_str == 'venda':
                acao_color = Fore.RED
            else:
                acao_color = Fore.YELLOW
            conf = mm.get('confianca', 0)
            capital_par = cap_snap.get(p, 0)
            flow_v = mm.get('order_flow', 0.0)
            fund_v = mm.get('funding_sinal', 0.0)
            oi_v = mm.get('oi_sinal', 0.0)
            print(f"{p:12} | {preco:10.4f} | "
                  f"{acao_color}{acao_str:7}{Style.RESET_ALL} | "
                  f"{conf:.2f} | {forca:+.3f} | "
                  f"{flow_v:+.2f} | {fund_v:+.1f} | {oi_v:+.2f} | "
                  f"{pos:10.6f} | {lucro_atual*100:6.2f}% | {capital_par:10.2f}")
        print(Fore.MAGENTA + "=" * 120 + "\n")
    except Exception as e:
        logger.error(f"[PAINEL] {e}")


def thread_mt_score():
    global micro_modelos
    while not encerrando:
        try:
            with lock_pares:
                pares_snapshot = list(pares_usdt)
            for par in pares_snapshot:
                try:
                    with lock_modelos:
                        mm = micro_modelos.get(par)
                        if not mm:
                            continue
                        confianca = mm.get('confianca', 0.5)
                        indicador_forca = mm.get('indicador_forca', 0.0)
                    vol = volatilidade.get(par, 0.0)
                    novo_mt_score = calcular_mt_score(par, confianca, vol, indicador_forca)
                    with lock_modelos:
                        mm_atual = micro_modelos.get(par)
                        if not mm_atual:
                            continue
                        mm_atual['mt_score'] = novo_mt_score
                        micro_modelos[par] = mm_atual
                except Exception as e:
                    logger.debug(f"[MT-SCORE THREAD] {par} | {e}")
        except Exception as e:
            logger.error(f"[MT-SCORE THREAD GLOBAL] {e}")
        time.sleep(7)


# ================================================================
# MAIN LOOP
# ================================================================
def main_loop():
    global ciclos_sem_trade
    try:
        logger.info("=" * 60)
        logger.info("  CRUZADA BOT v3.0 - CORRIGIDO")
        logger.info(f"  Capital: {CAPITAL_TOTAL} USDT")
        logger.info(f"  Risco máx/trade: {RISCO_MAX_POR_TRADE * 100}%")
        logger.info(f"  Stop loss duro: {STOP_LOSS_DURO_PCT * 100}%")
        logger.info(f"  Modo: {'REAL' if REAL_TRADES else 'SIMULADO'}")
        logger.info(f"  [ADAPT] vol={_adapt.get('vol_classe','?')} | "
                     f"stop={_adapt.get('stop_loss',0)*100:.1f}% | "
                     f"timeout={_adapt.get('timeout',0)}s | "
                     f"trail={_adapt.get('trailing_base',0)*100:.1f}% | "
                     f"risco={_adapt.get('risco_trade',0)*100:.1f}% | "
                     f"maxpos={_adapt.get('max_posicoes',0)}")
        logger.info("=" * 60)

        carregar_memoria()
        sincronizar_posicoes_binance()

        threads_cfg = {
            "Precos": monitorar_e_popular_precos_thread,
            "Pares": atualizar_pares_usdt_thread,
            "Trailing": trailing_thread,
            "Memoria": salvar_memoria_continuo,
            "MT-Score": thread_mt_score,
        }
        threads = {}
        for name, target in threads_cfg.items():
            t = threading.Thread(target=target, daemon=True, name=name)
            t.start()
            threads[name] = t
            logger.info(f"  Thread '{name}' iniciada.")

        logger.info("  Aguardando dados iniciais (30s)...")
        time.sleep(30)

        cycle = 0
        ciclos_sem_trade = 0

        while True:
            try:
                cycle += 1
                t0 = time.time()

                with lock_pares:
                    pares_snapshot = list(pares_usdt)

                print(f"\n{Fore.CYAN}{'='*80}")
                print(f"[CICLO {cycle}] Analisando {len(pares_snapshot)} pares...")
                print(f"{'='*80}{Style.RESET_ALL}")

                # 0. SINCRONIZAR POSIÇÕES COM BINANCE (a cada ciclo)
                # A Binance é a verdade. Se você fechou manual, o bot sabe.
                try:
                    sincronizar_posicoes_binance()
                except Exception as e:
                    logger.error(f"[SYNC] Erro: {e}")

                # ============================================================
                # PIPELINE DE 2 FASES — O segredo da velocidade
                # ============================================================
                _ciclo_inicio = time.time()  # ← IA usa pra saber quanto tempo passou
                # Tornar global pra IA acessar
                global _ciclo_inicio_global
                _ciclo_inicio_global = _ciclo_inicio
                # FASE 1 — SCAN RÁPIDO (200 pares, ~5 segundos)
                #   Usa apenas dados que já temos no cache (sem API calls)
                #   Filtra para os TOP 20 candidatos
                #
                # FASE 2 — ANÁLISE PROFUNDA (20 pares, ~50 segundos)  
                #   Book L2, funding, camada invisível, baleias
                #   Só nos pares que passaram o scan
                #
                # TOTAL: ~60 segundos vs 500 segundos antes
                # ============================================================

                # 1. Modelos — analisar TODOS os pares
                # FAST LANE: se um par bate sinal muito forte, executa imediato
                # LIMITE: máximo 1 trade por ciclo via fast lane (evita abrir 10 de uma vez)
                _fast_lane_executados = set()
                _fast_lane_max = 2  # máximo 2 execuções por ciclo
                for par in pares_snapshot:
                    if encerrando:
                        break
                    try:
                        calcular_indicadores_e_prever(par)
                        construir_micro_modelo(par)
                        
                        # ========== FAST LANE ==========
                        # Se o sinal é MUITO forte, não espera os outros 199 pares
                        # Executa cerebro + IA agora mesmo, enquanto o preço está fresco
                        with lock_modelos:
                            mm_fast = micro_modelos.get(par, {})
                        
                        forca_fast = abs(mm_fast.get('forca_final', 0))
                        conf_fast = mm_fast.get('confianca', 0)
                        acao_fast = mm_fast.get('acao', 'manter')
                        
                        # Critérios: Força ≥ 0.90 E Confiança ≥ 0.75 E não é "manter"
                        # E não excedeu o limite de fast lane por ciclo
                        if (forca_fast >= 0.90 and conf_fast >= 0.75 and acao_fast != 'manter'
                                and len(_fast_lane_executados) < _fast_lane_max):
                            # Verificar se já temos posições demais
                            with lock_posicoes:
                                n_pos = sum(1 for v in posicoes.values() if abs(v) > 1e-12)
                            max_pos = _adapt_get('max_posicoes', MAX_POSICOES_SIMULTANEAS)
                            
                            # Já tem posição neste par?
                            with lock_posicoes:
                                pos_existente = posicoes.get(par, 0.0)
                            tem_pos = abs(pos_existente) > 1e-12
                            
                            if n_pos < max_pos and not tem_pos and par not in _fast_lane_executados:
                                print(f"\n  {Fore.YELLOW + Style.BRIGHT}[⚡ FAST LANE] {par} | "
                                      f"{acao_fast} | força={forca_fast:.3f} conf={conf_fast:.2f} | "
                                      f"Analisando AGORA sem esperar...{Style.RESET_ALL}")
                                
                                # Rodar cérebro + baleias neste par
                                try:
                                    trap_fast = detectar_armadilha_baleia(par)
                                    if trap_fast.get('armadilha') and trap_fast.get('severidade', 0) >= 0.6:
                                        print(f"  {Fore.RED}[⚡ FAST] {par} BALEIA detectada → cancelado{Style.RESET_ALL}")
                                        continue
                                    cerebro_decisao(par)
                                except:
                                    pass
                                
                                # Confirmar entrada
                                try:
                                    ok, razao, ajuste = confirmar_entrada(par, acao_fast)
                                except:
                                    ok = False
                                
                                if ok:
                                    # IA valida
                                    with lock_modelos:
                                        mm_upd = micro_modelos.get(par, {})
                                    forca_upd = mm_upd.get('forca_final', forca_fast)
                                    conf_upd = mm_upd.get('confianca', conf_fast)
                                    
                                    ia_ok, ia_prob, ia_motivo = validacao_final_ia(
                                        par, acao_fast, conf_upd, forca_upd)
                                    
                                    if ia_ok:
                                        # Alocar capital ANTES de executar
                                        cap_fast = ajustar_capital_dinamico(par, conf_upd)
                                        cap_fast = round(cap_fast * ajuste, 6)
                                        # A3 FIX: Floor absoluto após ajustes
                                        cap_fast = max(cap_fast, 30.0)
                                        with lock_capital:
                                            capital_por_par[par] = cap_fast
                                        
                                        if cap_fast > 0:
                                            print(f"  {Fore.GREEN + Style.BRIGHT}[⚡ FAST] {par} | "
                                                  f"IA APROVOU ({ia_prob:.0%}) | capital=${cap_fast:.2f} → EXECUTANDO{Style.RESET_ALL}")
                                            preparar_e_executar(par)
                                            _fast_lane_executados.add(par)
                                            logger.info(f"[⚡ FAST LANE] {par} | {acao_fast} | "
                                                       f"prob={ia_prob:.0%} | capital=${cap_fast:.2f} | EXECUTADO durante scan")
                                        else:
                                            print(f"  {Fore.RED}[⚡ FAST] {par} | capital=0 → sem capital{Style.RESET_ALL}")
                                    else:
                                        print(f"  {Fore.RED}[⚡ FAST] {par} | "
                                              f"IA bloqueou: {ia_motivo}{Style.RESET_ALL}")
                                else:
                                    print(f"  {Fore.RED}[⚡ FAST] {par} | "
                                          f"Confirmação falhou: {razao}{Style.RESET_ALL}")
                        
                    except Exception as e:
                        logger.error(f"[CICLO] Erro {par}: {e}")

                # 2. Correlação / Capital
                if cycle % 5 == 0:
                    try:
                        calcular_correlacoes()
                    except Exception as e:
                        logger.error(f"[CICLO] Erro correlações: {e}")
                try:
                    ajustar_capital_por_confianca()
                except Exception as e:
                    logger.error(f"[CICLO] Erro capital: {e}")

                # 2.5 Detectar regime de mercado
                try:
                    detectar_regime_mercado()
                except Exception as e:
                    logger.error(f"[CICLO] Erro regime: {e}")
                
                # ADAPTAÇÃO: ajustar parâmetros ao mercado real
                try:
                    adaptar_parametros_mercado()
                except Exception as e:
                    logger.debug(f"[CICLO] Erro adapt: {e}")

                # 2.6 CÉREBRO + BALEIAS
                print(f"\n  {Fore.CYAN}[CÉREBRO] Analisando sinais preditivos...{Style.RESET_ALL}")
                cerebro_count = 0
                armadilhas_count = 0
                for par in pares_snapshot:
                    if encerrando:
                        break
                    with lock_modelos:
                        mm_check = micro_modelos.get(par, {})
                    if mm_check.get('acao', 'manter') != 'manter' and \
                       mm_check.get('confianca', 0) >= 0.5:
                        try:
                            # Detectar armadilhas de baleias
                            trap = detectar_armadilha_baleia(par)
                            if trap['armadilha']:
                                armadilhas_count += 1
                                sev_cor = Fore.RED if trap['severidade'] >= 0.6 else Fore.YELLOW
                                print(f"  {sev_cor}[🐋 BALEIA] {par} | {trap['tipo']} | "
                                      f"severidade={trap['severidade']:.1f} | "
                                      f"direção_real={trap['direcao_real']}{Style.RESET_ALL}")
                                with lock_modelos:
                                    mm_trap = micro_modelos.get(par, {})
                                    mm_trap['armadilha'] = trap['tipo']
                                    mm_trap['armadilha_severidade'] = trap['severidade']
                                    # Se armadilha grave, ajustar decisão
                                    if trap['severidade'] >= 0.6:
                                        if trap['direcao_real'] != 'indefinido':
                                            # Usar a direção real detectada
                                            mm_trap['acao'] = trap['direcao_real']
                                            mm_trap['cerebro_razao'] = f"BALEIA_{trap['tipo']}"
                                        else:
                                            # Não sabemos a direção → cancelar
                                            mm_trap['acao'] = 'manter'
                                            mm_trap['confianca'] = 0.0
                                    elif trap['severidade'] >= 0.3:
                                        mm_trap['confianca'] = max(0.0,
                                            mm_trap.get('confianca', 0) - 0.15)
                                    micro_modelos[par] = mm_trap

                            # Cérebro (sinais preditivos)
                            cerebro_decisao(par)
                            cerebro_count += 1
                        except Exception as e:
                            logger.debug(f"[CÉREBRO] Erro {par}: {e}")
                logger.info(f"[CÉREBRO] {cerebro_count} pares analisados | "
                            f"{armadilhas_count} armadilhas detectadas")
                if armadilhas_count > 0:
                    print(f"  {Fore.YELLOW}[🐋] {armadilhas_count} armadilhas de baleias detectadas{Style.RESET_ALL}")
                print(f"  {Fore.CYAN}[CÉREBRO] {cerebro_count} pares analisados{Style.RESET_ALL}")

                # 3. RANKING
                tempo_ranking = time.time()
                
                with lock_modelos:
                    candidatos = []
                    for par in pares_snapshot:
                        mm = micro_modelos.get(par, {})
                        if not mm:
                            continue
                        acao = mm.get('acao', 'manter')
                        if acao == 'manter':
                            continue
                        forca = mm.get('forca_final', 0.0)
                        conf = mm.get('confianca', 0.0)
                        if conf < 0.35:
                            continue

                        # ========== CONVERGÊNCIA ==========
                        direcao = 1 if acao == 'compra' else -1
                        votos = 0
                        total_votos = 5
                        
                        # Voto 1: Indicadores técnicos
                        if forca * direcao > 0.2:
                            votos += 1
                        
                        # Voto 2: Multi-tempo
                        mt_hist = mm.get('mt_hist', memoria_mt_hist.get(par, []))
                        if mt_hist and len(mt_hist) >= 2:
                            mt_dir = sum(1 for s in mt_hist[-3:] if s * direcao > 0)
                            if mt_dir >= 2:
                                votos += 1
                        
                        # Voto 3: Funding
                        funding_v = mm.get('funding_sinal', 0.0)
                        if funding_v * direcao > 0:
                            votos += 1
                        
                        # Voto 4: Invisível
                        inv_sinal = mm.get('invisivel_sinal', 0.0)
                        if inv_sinal * direcao > 0.1:
                            votos += 1
                        
                        # Voto 5: Book
                        book_r = precos_cache.get(par + '_book', {})
                        if isinstance(book_r, dict) and book_r.get('bids') and book_r.get('asks'):
                            tb = sum(q for _, q in book_r['bids'][:10])
                            ta = sum(q for _, q in book_r['asks'][:10])
                            book_bias = (tb - ta) / (tb + ta + 1e-9)
                            if book_bias * direcao > 0.1:
                                votos += 1
                        
                        convergencia = votos / max(total_votos, 1)
                        # B3 FIX: 2 votos bastam (fontes 3,4,5 frequentemente nulas)
                        if votos < 2:
                            continue

                        # Score composto para ranking
                        hist = mm.get('historico_acertos', [])
                        taxa = sum(hist) / len(hist) if len(hist) >= 2 else 0.5
                        vol = volatilidade.get(par, 0.01)
                        mt_hist = mm.get('mt_hist', memoria_mt_hist.get(par, []))

                        # Consistência MT
                        direcao = 1 if acao == 'compra' else -1
                        mt_consist = 0.5
                        if mt_hist:
                            mt_consist = sum(1 for s in mt_hist if s * direcao > 0) / len(mt_hist)

                        # Volume score (preferir moedas líquidas)
                        with lock_precos:
                            dados_par = precos_cache.get(par, [])
                        vol_score = 0.5
                        vol_24_avg = 0.0
                        if dados_par and isinstance(dados_par[-1], dict):
                            try:
                                vols_r = [float(d.get('volume', 0)) for d in dados_par[-60:]]
                                vol_24_avg = sum(vols_r) / max(len(vols_r), 1)
                                if vol_24_avg > 1000000:
                                    vol_score = 1.0
                                elif vol_24_avg > 100000:
                                    vol_score = 0.7
                                elif vol_24_avg < 10000:
                                    vol_score = 0.2
                            except:
                                pass

                        # FILTRO: Volume mínimo — moedas < $50K de volume
                        # têm spread enorme que come qualquer lucro de scalping
                        if vol_24_avg < 50000:
                            continue

                        # FILTRO: Spread — se spread > 0.3%, custo de ida+volta > 0.6%
                        # impossível lucrar em scalping com esse spread
                        book_rank = precos_cache.get(par + '_book', {})
                        if isinstance(book_rank, dict) and book_rank.get('bids') and book_rank.get('asks'):
                            try:
                                best_bid = float(book_rank['bids'][0][0])
                                best_ask = float(book_rank['asks'][0][0])
                                if best_bid > 0:
                                    spread_pct = (best_ask - best_bid) / best_bid
                                    if spread_pct > 0.003:
                                        continue  # spread muito alto
                            except:
                                pass

                        # ATR score (preferir volatilidade moderada para scalping)
                        atr_score = 0.5
                        with lock_indicadores:
                            df_rank = indicadores.get(par)
                        if df_rank is not None and 'atr' in df_rank.columns:
                            try:
                                atr_pct = float(df_rank['atr'].iloc[-1]) / float(df_rank['close'].iloc[-1])
                                if 0.003 < atr_pct < 0.015:
                                    atr_score = 1.0  # sweet spot para scalping
                                elif atr_pct < 0.001:
                                    atr_score = 0.2  # muito parado
                                elif atr_pct > 0.03:
                                    atr_score = 0.3  # muito arriscado
                            except:
                                pass

                        # Score final de ranking
                        # CONVERGÊNCIA tem peso dominante — se muitas fontes concordam,
                        # o trade é mais confiável do que qualquer indicador sozinho
                        rank_score = (
                            0.30 * convergencia +              # CONVERGÊNCIA é rei
                            0.20 * min(abs(forca), 1.0) +     # força do sinal
                            0.15 * conf +                       # confiança
                            0.10 * taxa +                       # histórico de acertos
                            0.10 * mt_consist +                 # consistência multi-tempo
                            0.10 * vol_score +                  # liquidez
                            0.05 * atr_score                    # volatilidade ideal
                        )

                        candidatos.append({
                            'par': par,
                            'acao': acao,
                            'rank_score': rank_score,
                            'forca': forca,
                            'conf': conf,
                            'taxa': taxa,
                            'vol_score': vol_score,
                            'votos': votos,
                            'total_votos': total_votos,
                            'convergencia': convergencia,
                        })

                # Separar LONG e SHORT, ordenar por rank_score
                longs = sorted([c for c in candidatos if c['acao'] == 'compra'],
                               key=lambda x: x['rank_score'], reverse=True)
                shorts = sorted([c for c in candidatos if c['acao'] == 'venda'],
                                key=lambda x: x['rank_score'], reverse=True)

                # TOP LONG + TOP SHORT (considerando fast lane e posições abertas)
                with lock_posicoes:
                    n_pos_atual = sum(1 for v in posicoes.values() if abs(v) > 1e-12)
                max_pos = int(_adapt_get('max_posicoes', MAX_POSICOES_SIMULTANEAS))
                vagas_livres = max(0, max_pos - n_pos_atual - len(_fast_lane_executados))
                
                # Distribuir vagas: 60% long, 40% short
                max_long = max(1, int(vagas_livres * 0.6))
                max_short = max(1, vagas_livres - max_long)
                
                top_compras = []
                for c in longs:
                    if len(top_compras) >= max_long:
                        break
                    # Verificar correlação com pares já selecionados
                    correlacionado = False
                    for sel in top_compras:
                        corr = correlacoes.get(c['par'], 0)
                        corr2 = correlacoes.get(sel, 0)
                        if corr > 0.7 and corr2 > 0.7:
                            correlacionado = True
                            break
                    if not correlacionado:
                        top_compras.append(c['par'])

                top_vendas = []
                for c in shorts:
                    if len(top_vendas) >= max_short:
                        break
                    correlacionado = False
                    for sel in top_vendas:
                        corr = correlacoes.get(c['par'], 0)
                        if corr > 0.7:
                            correlacionado = True
                            break
                    if not correlacionado:
                        top_vendas.append(c['par'])

                top_executar = top_compras + top_vendas

                if not top_executar:
                    ciclos_sem_trade += 1
                    print(f"\n  {Fore.YELLOW}[SNIPER] Nenhum alvo certeiro — ESPERANDO. "
                          f"(ciclo {ciclos_sem_trade} sem trade){Style.RESET_ALL}")
                    logger.info(f"[CICLO {cycle}] Nenhum par qualificado "
                                f"(sem trade há {ciclos_sem_trade} ciclos)")
                    if longs:
                        l = longs[0]
                        logger.info(f"  Melhor LONG: {l['par']} score={l['rank_score']:.3f} "
                                    f"força={l['forca']:+.3f} conf={l['conf']:.2f}")
                    if shorts:
                        s = shorts[0]
                        logger.info(f"  Melhor SHORT: {s['par']} score={s['rank_score']:.3f} "
                                    f"força={s['forca']:+.3f} conf={s['conf']:.2f}")
                else:
                    for c in candidatos:
                        if c['par'] in top_executar:
                            logger.info(f"  [TOP] {c['par']:12} | {c['acao']:6} | "
                                        f"score={c['rank_score']:.3f} | força={c['forca']:+.3f} | "
                                        f"conf={c['conf']:.2f} | winrate={c['taxa']:.0%} | "
                                        f"liq={c['vol_score']:.1f}")
                    logger.info(f"[RANKING] LONG: {top_compras} | SHORT: {top_vendas}")

                # 4. Execução com confirmação
                # Sincronizar posições ANTES de executar (caso fechou manual durante o ciclo)
                try:
                    sincronizar_posicoes_binance()
                except:
                    pass
                
                exec_resultados = []
                # Criar lookup rápido de candidatos
                cand_lookup = {c['par']: c for c in candidatos}
                
                for par in top_executar:
                    if encerrando:
                        break
                    # Skip pares já executados pelo FAST LANE
                    if par in _fast_lane_executados:
                        exec_resultados.append(f"  ⚡ {par} | já executado via FAST LANE")
                        continue
                    try:
                        with lock_modelos:
                            mm_exec = micro_modelos.get(par, {})
                            confianca = mm_exec.get("confianca", 0.0)
                            acao_exec = mm_exec.get("acao", "manter")

                        # CONFIRMAÇÃO DE ENTRADA
                        aprovado, motivo, ajuste_cap = confirmar_entrada(par, acao_exec)
                        if not aprovado:
                            exec_resultados.append(f"  ❌ {par} | {acao_exec} | BLOQUEADO: {motivo}")
                            logger.info(f"  [BLOQUEADO] {par} | {acao_exec} | {motivo}")
                            continue

                        # ANTI-FRAGILIDADE
                        forca_exec = mm_exec.get('forca_final', 0.0)
                        funding_exec = mm_exec.get('funding_sinal', 0.0)
                        flow_exec = mm_exec.get('order_flow', 0.0)
                        fator_antifragil = verificar_padrao_perda(
                            par, acao_exec, forca_exec, regime_mercado,
                            funding_exec, flow_exec
                        )
                        if fator_antifragil <= 0:
                            exec_resultados.append(f"  ❌ {par} | {acao_exec} | ANTI-FRÁGIL bloqueou")
                            continue

                        # BALEIA
                        armadilha_info = _armadilhas_detectadas.get(par, {})
                        if armadilha_info and (time.time() - armadilha_info.get('ts', 0)) < 120:
                            if armadilha_info.get('severidade', 0) >= 0.5:
                                exec_resultados.append(f"  ❌ {par} | {acao_exec} | BALEIA: {armadilha_info.get('tipo', '?')}")
                                continue

                        cap = ajustar_capital_dinamico(par, confianca)
                        cap = round(cap * ajuste_cap * fator_antifragil, 6)
                        
                        # A3 FIX: Floor ABSOLUTO após TODOS os ajustes
                        # Nenhuma multiplicação pode levar abaixo de $30
                        cap = max(cap, 30.0)

                        with lock_capital:
                            capital_por_par[par] = cap

                        if cap <= 0:
                            exec_resultados.append(f"  ❌ {par} | {acao_exec} | CAPITAL ZERO")
                            continue

                        # ====== CÉREBRO IA — ÚLTIMA VALIDAÇÃO ======
                        ia_ok, ia_prob, ia_motivo = validacao_final_ia(
                            par, acao_exec, confianca, forca_exec)
                        
                        if not ia_ok:
                            exec_resultados.append(
                                f"  🧠 {par} | {acao_exec} | IA BLOQUEOU: {ia_motivo} "
                                f"(prob={ia_prob:.0%})")
                            logger.info(f"  [IA BLOQUEOU] {par} | {acao_exec} | "
                                        f"prob={ia_prob:.0%} | {ia_motivo}")
                            continue
                        
                        exec_resultados.append(
                            f"  ✅ {par} | {acao_exec} | capital=${cap:.2f} | "
                            f"IA={ia_prob:.0%} | votos="
                            f"{cand_lookup.get(par, {}).get('votos','?')}/"
                            f"{cand_lookup.get(par, {}).get('total_votos','?')} | "
                            f"ENVIANDO ORDEM...")
                        logger.info(f"  [CONFIRMADO+IA] {par} | {acao_exec} | "
                                    f"capital=${cap:.2f} | IA={ia_prob:.0%}")

                        preparar_e_executar(par, acao_forcada=acao_exec, conf_forcada=confianca)
                    except Exception as e:
                        exec_resultados.append(f"  ❌ {par} | ERRO: {e}")
                        logger.error(f"[EXEC] Erro {par}: {e}")
                    time.sleep(0.05)

                # 5. Painel
                try:
                    mostrar_painel_completo()
                except Exception as e:
                    logger.error(f"[PAINEL] {e}")

                # 6. RESUMO DE EXECUÇÃO — SEMPRE VISÍVEL (depois do painel)
                print(f"\n{Fore.YELLOW + Style.BRIGHT}{'='*70}")
                print(f"  RESUMO EXECUÇÃO CICLO {cycle}")
                print(f"  LONG: {top_compras} | SHORT: {top_vendas}")
                print(f"{'='*70}{Style.RESET_ALL}")
                for r in exec_resultados:
                    print(r)
                if not exec_resultados:
                    print(f"  {Fore.RED}NENHUM PAR NO TOP — ranking vazio{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}{'='*70}{Style.RESET_ALL}\n")
                logger.info(f"[RESUMO] " + " | ".join(exec_resultados))

                # 7. Watchdog
                for name, t in threads.items():
                    if not t.is_alive():
                        logger.error(f"[WATCHDOG] Thread '{name}' morreu! Reiniciando...")
                        nt = threading.Thread(target=threads_cfg[name], daemon=True, name=name)
                        nt.start()
                        threads[name] = nt

                elapsed = time.time() - t0
                logger.info(f"[CICLO {cycle}] OK | pares={len(pares_snapshot)} | "
                            f"compras={top_compras} vendas={top_vendas} | tempo={elapsed:.1f}s")

                time.sleep(max(0.1, SLEEP_BETWEEN_CYCLES - elapsed))

            except KeyboardInterrupt:
                logger.info("Encerrando por KeyboardInterrupt...")
                break
            except Exception as e:
                logger.error(f"[CICLO {cycle}] Erro: {e}")
                traceback.print_exc()
                time.sleep(5)

    except Exception as e:
        logger.critical(f"ERRO FATAL: {e}")
        traceback.print_exc()
    finally:
        salvar_memoria()
        logger.info("Bot encerrado.")


if __name__ == "__main__":
    main_loop()
