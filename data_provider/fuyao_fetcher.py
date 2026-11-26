#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""同花顺 Fuyao 数据源 Fetcher

基于同花顺开源数据服务 (https://fuyao.aicubes.cn) 实现，
提供 A 股日K线、实时行情、指数行情、涨停池、板块排行等能力。

API 文档: https://fuyao.aicubes.cn/llms-full.txt

环境变量:
    FUYAO_API_KEY: 同花顺 Fuyao API Key（sk-fuyao-... 格式）
"""

import json
import logging
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .base import BaseFetcher, STANDARD_COLUMNS, normalize_stock_code
from .realtime_types import RealtimeSource, UnifiedRealtimeQuote

logger = logging.getLogger(__name__)

FUYAO_BASE_URL = "https://fuyao.aicubes.cn"
FUYAO_TIMEOUT = 30
FUYAO_RETRY_CODES = {4001, 5001, 5002, 5003}
FUYAO_MAX_RETRIES = 3


def _code_to_thscode(stock_code: str) -> str:
    """将纯数字股票代码转为 fuyao thscode（带市场后缀）"""
    code = normalize_stock_code(stock_code)
    code = code.replace(".", "")
    if code.startswith(("6", "9", "5")):
        return f"{code}.SH"
    if code.startswith(("0", "3", "2")):
        return f"{code}.SZ"
    if code.startswith(("8", "4")):
        return f"{code}.BJ"
    return f"{code}.SH"


def _fuyao_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """调用 fuyao API，支持自动重试"""
    api_key = _get_api_key()
    if not api_key:
        logger.warning("[Fuyao] 未配置 FUYAO_API_KEY，跳过调用")
        return {"code": -1, "message": "no api key"}

    url = f"{FUYAO_BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    headers = {
        "X-api-key": api_key,
        "Accept": "application/json",
    }

    for attempt in range(FUYAO_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, method="GET", headers=headers)
            with urllib.request.urlopen(req, timeout=FUYAO_TIMEOUT) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            code = body.get("code", 0)
            if code == 0:
                return body
            if code in FUYAO_RETRY_CODES and attempt < FUYAO_MAX_RETRIES:
                logger.warning("[Fuyao] retryable code=%s attempt=%d", code, attempt + 1)
                time.sleep(1 * (attempt + 1))
                continue
            logger.warning("[Fuyao] API error: code=%s message=%s", code, body.get("message"))
            return body
        except Exception as e:
            logger.warning("[Fuyao] request failed: %s attempt=%d", e, attempt + 1)
            if attempt < FUYAO_MAX_RETRIES:
                time.sleep(1 * (attempt + 1))
                continue
            return {"code": -1, "message": str(e)}

    return {"code": -1, "message": "all retries exhausted"}


def _get_api_key() -> Optional[str]:
    """从环境变量读取 API Key"""
    import os
    return os.environ.get("FUYAO_API_KEY", "").strip() or None


class FuyaoFetcher(BaseFetcher):
    """同花顺 Fuyao 数据源"""

    name: str = "FuyaoFetcher"
    priority: int = 2  # 仅次于 TickPlus，优先于 akshare/efinance

    # ------------------------------------------------------------------
    # 日线数据
    # ------------------------------------------------------------------

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从 fuyao 获取历史日K线"""
        thscode = _code_to_thscode(stock_code)

        start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ms = int(datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59).timestamp() * 1000)

        logger.info("[Fuyao] fetching daily data: %s (%s ~ %s)", stock_code, start_date, end_date)
        result = _fuyao_get("/api/a-share/prices/historical", {
            "thscode": thscode,
            "interval": "1d",
            "start": start_ms,
            "end": end_ms,
            "adjust": "forward",
        })

        if result.get("code") != 0:
            logger.warning("[Fuyao] daily data failed for %s: %s", stock_code, result.get("message"))
            return pd.DataFrame()

        items = result.get("data", {}).get("item", [])
        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)
        logger.info("[Fuyao] got %d bars for %s", len(df), stock_code)
        return df

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化 fuyao 日K线数据列名"""
        if df is None or df.empty:
            return df

        df = df.copy()
        # fuyao 字段映射到标准列名
        rename_map = {
            "date_ms": "date",
            "open_price": "open",
            "high_price": "high",
            "low_price": "low",
            "close_price": "close",
            "volume": "volume",
            "turnover": "amount",
        }
        df = df.rename(columns=rename_map)

        # date_ms 转日期字符串
        if "date_ms" in df.columns:
            df["date"] = pd.to_datetime(df["date_ms"], unit="ms").dt.strftime("%Y-%m-%d")
            df = df.drop(columns=["date_ms"])

        # 计算涨跌幅
        if "close" in df.columns and len(df) > 1:
            df["prev_close"] = df["close"].shift(1)
            df["pct_chg"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100
            df = df.drop(columns=["prev_close"])
        elif "pct_chg" not in df.columns:
            df["pct_chg"] = 0.0

        # 确保标准列存在
        for col in STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = 0

        return df[STANDARD_COLUMNS]

    # ------------------------------------------------------------------
    # 实时行情
    # ------------------------------------------------------------------

    def get_realtime_quote(self, stock_code: str, *, log_final_failure: bool = True,
                           analysis_target=None) -> Optional[UnifiedRealtimeQuote]:
        """获取实时行情"""
        thscode = _code_to_thscode(stock_code)
        result = _fuyao_get("/api/a-share/prices/snapshot", {"thscodes": thscode})

        if result.get("code") != 0:
            return None

        items = result.get("data", {}).get("item", [])
        if not items:
            return None

        item = items[0]
        prev_price = item.get("prev_price")
        last_price = item.get("last_price")
        change_pct = item.get("price_change_ratio_pct")

        quote = UnifiedRealtimeQuote(
            code=normalize_stock_code(stock_code),
            source=RealtimeSource.FUYAO,
            price=last_price,
            change_pct=change_pct,
            change_amount=item.get("price_change"),
            volume=item.get("volume"),
            amount=item.get("turnover"),
            open_price=item.get("open_price"),
            high=item.get("high_price"),
            low=item.get("low_price"),
            pre_close=prev_price,
        )
        return quote

    # ------------------------------------------------------------------
    # 指数行情
    # ------------------------------------------------------------------

    def get_main_indices(self, region: str = "cn") -> Optional[List[Dict[str, Any]]]:
        """获取主要指数实时行情"""
        if region != "cn":
            return None

        # 主要 A 股指数 thscode
        index_codes = {
            "000001.SH": "上证指数",
            "399001.SZ": "深证成指",
            "399006.SZ": "创业板指",
            "000688.SH": "科创50",
            "000016.SH": "上证50",
            "000300.SH": "沪深300",
        }
        thscodes = ",".join(index_codes.keys())

        result = _fuyao_get("/api/a-share-index/prices/snapshot", {"thscodes": thscodes})
        if result.get("code") != 0:
            return None

        items = result.get("data", {}).get("item", [])
        if not items:
            return None

        results = []
        for item in items:
            thscode = item.get("thscode", "")
            name = index_codes.get(thscode, thscode)
            results.append({
                "code": thscode.split(".")[0],
                "name": name,
                "current": item.get("last_price"),
                "change": item.get("price_change"),
                "change_pct": item.get("price_change_ratio_pct"),
                "volume": item.get("volume"),
                "amount": item.get("turnover"),
            })
        return results

    # ------------------------------------------------------------------
    # 市场涨跌统计
    # ------------------------------------------------------------------

    def get_market_stats(self, *, allow_cache: bool = True) -> Optional[Dict[str, Any]]:
        """获取市场涨跌统计"""
        # 获取全市场行情快照（分页拉取）
        all_items = []
        offset = 0
        limit = 100

        while True:
            result = _fuyao_get("/api/a-share/prices/snapshot", {
                "limit": limit,
                "offset": offset,
            })
            if result.get("code") != 0:
                break

            data = result.get("data", {})
            items = data.get("item", [])
            all_items.extend(items)

            total = data.get("total", 0)
            if offset + limit >= total or not items:
                break
            offset += limit

        if not all_items:
            return None

        # 统计涨跌
        up_count = 0
        down_count = 0
        flat_count = 0
        limit_up_count = 0
        limit_down_count = 0
        total_amount = 0.0

        for item in all_items:
            last_price = item.get("last_price")
            prev_price = item.get("prev_price")
            change_pct = item.get("price_change_ratio_pct")
            turnover = item.get("turnover")

            if last_price is None or prev_price is None or prev_price <= 0:
                continue

            if turnover and turnover > 0:
                total_amount += turnover

            if change_pct is not None:
                if change_pct > 0:
                    up_count += 1
                elif change_pct < 0:
                    down_count += 1
                else:
                    flat_count += 1

                # 涨停/跌停判断（A 股规则：10%/20%）
                code = item.get("thscode", "")
                pure_code = normalize_stock_code(code)
                if pure_code.startswith(("688", "30")):
                    ratio = 0.20
                elif "ST" in item.get("name", ""):
                    ratio = 0.05
                else:
                    ratio = 0.10

                if abs(change_pct) >= ratio * 100 - 0.01:
                    if change_pct > 0:
                        limit_up_count += 1
                    else:
                        limit_down_count += 1

        if up_count + down_count + flat_count == 0:
            return None

        stats = {
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "limit_up_count": limit_up_count,
            "limit_down_count": limit_down_count,
            "total_amount": total_amount / 1e8,  # 转为亿元
        }
        logger.info("[Fuyao] market stats: up=%s down=%s flat=%s limit_up=%s limit_down=%s amount=%.0f亿",
                    up_count, down_count, flat_count, limit_up_count, limit_down_count,
                    stats["total_amount"])
        return stats

    # ------------------------------------------------------------------
    # 涨停池
    # ------------------------------------------------------------------

    def get_limit_up_pool(self, date: Optional[str] = None, n: int = 20) -> Optional[List[Dict[str, Any]]]:
        """获取涨停池"""
        result = _fuyao_get("/api/a-share/special-data/limit-up-pool", {})
        if result.get("code") != 0:
            return None

        items = result.get("data", {}).get("item", [])
        if not items:
            return None

        pool = []
        for item in items[:n]:
            pool.append({
                "code": normalize_stock_code(item.get("thscode", "")),
                "name": item.get("name", ""),
                "price": item.get("last_price"),
                "change_pct": item.get("price_change_ratio_pct"),
                "limit_up_time": item.get("limit_up_time"),
                "continue_day_cnt": item.get("continue_day_cnt"),
                "seal_money": item.get("seal_money"),
            })
        return pool

    # ------------------------------------------------------------------
    # 板块排行
    # ------------------------------------------------------------------

    def get_sector_rankings(self, n: int = 5) -> Optional[Tuple[List[Dict], List[Dict]]]:
        """获取行业板块涨跌榜"""
        # 获取同花顺行业指数列表
        result = _fuyao_get("/api/a-share-index/catalog/ths-index-list", {"tag": "industry"})
        if result.get("code") != 0:
            return None

        items = result.get("data", {}).get("item", [])
        if not items:
            return None

        # 获取每个指数的快照
        if not items:
            return None

        # 取前 50 个板块指数的快照
        thscodes = ",".join(item.get("thscode", "") for item in items[:50] if item.get("thscode"))
        if not thscodes:
            return None

        snap_result = _fuyao_get("/api/a-share-index/prices/snapshot", {"thscodes": thscodes})
        if snap_result.get("code") != 0:
            return None

        snap_items = snap_result.get("data", {}).get("item", [])
        if not snap_items:
            return None

        ranked = []
        for item in snap_items:
            ranked.append({
                "name": item.get("thscode", ""),
                "change_pct": item.get("price_change_ratio_pct", 0.0),
            })

        ranked.sort(key=lambda x: x.get("change_pct", 0), reverse=True)
        top = ranked[:n]
        bottom = ranked[-n:] if len(ranked) > n else ranked[:n]
        bottom = sorted(bottom, key=lambda x: x.get("change_pct", 0))

        return top, bottom

    # ------------------------------------------------------------------
    # 概念排行
    # ------------------------------------------------------------------

    def get_concept_rankings(self, n: int = 5) -> Optional[Tuple[List[Dict], List[Dict]]]:
        """获取概念/题材涨跌榜"""
        result = _fuyao_get("/api/a-share-index/catalog/ths-index-list", {"tag": "cn_concept"})
        if result.get("code") != 0:
            return None

        items = result.get("data", {}).get("item", [])
        if not items:
            return None

        thscodes = ",".join(item.get("thscode", "") for item in items[:50] if item.get("thscode"))
        if not thscodes:
            return None

        snap_result = _fuyao_get("/api/a-share-index/prices/snapshot", {"thscodes": thscodes})
        if snap_result.get("code") != 0:
            return None

        snap_items = snap_result.get("data", {}).get("item", [])
        if not snap_items:
            return None

        ranked = []
        for item in snap_items:
            ranked.append({
                "name": item.get("thscode", ""),
                "change_pct": item.get("price_change_ratio_pct", 0.0),
            })

        ranked.sort(key=lambda x: x.get("change_pct", 0), reverse=True)
        top = ranked[:n]
        bottom = ranked[-n:] if len(ranked) > n else ranked[:n]
        bottom = sorted(bottom, key=lambda x: x.get("change_pct", 0))

        return top, bottom


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetcher = FuyaoFetcher()
    # 测试日K线
    df = fetcher.get_daily_data("600519", days=10)
    print(f"\n日K线: {len(df)} 行")
    if not df.empty:
        print(df.tail(3).to_string())
    # 测试指数
    indices = fetcher.get_main_indices("cn")
    print(f"\n指数: {len(indices)} 个")
    for idx in indices:
        print(f"  {idx['name']}: {idx['current']} ({idx['change_pct']:+.2f}%)")
