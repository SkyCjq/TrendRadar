# coding=utf-8

"""
RSS 抓取器

扩展能力：
1. allowed_authors
   对 RSSHub keyword 结果做官方作者精确白名单过滤。

2. ticket_enrich
   对篮球票务相关微博中的外链进行跟随。

3. 严格票务项目页识别
   微博内部页、微博访客登录页、图片 CDN、普通媒体文章
   不再被误判成“真实票务项目页”。

4. 票星球 HTTP 469
   如果已经发现真实 m.piaoxingqiu.com 项目链接，但 GitHub Actions
   请求返回 469，则保留项目 URL、lssId 和“自动读取受限”状态，
   而不是把该项目整体丢弃。
"""

import html
import json
import random
import re
import time

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .parser import RSSParser
from trendradar.storage.base import RSSItem, RSSData
from trendradar.utils.time import get_configured_time, DEFAULT_TIMEZONE


@dataclass
class RSSFeedConfig:
    """RSS 源配置"""

    id: str
    name: str
    url: str
    max_items: int = 0
    enabled: bool = True
    max_age_days: Optional[int] = None
    allowed_authors: List[str] = field(default_factory=list)
    ticket_enrich: bool = False


class _VisibleTextParser(HTMLParser):
    """从 HTML 中提取可见文字"""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._parts: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in {"script", "style", "noscript", "svg"}:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if (
            tag.lower() in {"script", "style", "noscript", "svg"}
            and self._skip_depth > 0
        ):
            self._skip_depth -= 1

    def handle_data(self, data):
        if self._skip_depth != 0:
            return

        text = re.sub(r"\s+", " ", data).strip()
        if text:
            self._parts.append(text)

    def get_text(self) -> str:
        return re.sub(r"\s+", " ", " ".join(self._parts)).strip()


class RSSFetcher:
    """RSS 抓取器"""

    RSSHUB_FALLBACK_FROM = "https://rsshub.app/"
    RSSHUB_FALLBACK_TO = "https://rsshub.rss3.workers.dev/"

    TICKET_STRONG_WORDS = (
        "购票",
        "售票",
        "开票",
        "票价",
        "票务",
        "开售",
        "预售",
        "抢票",
        "开抢",
        "抢购",
        "余票",
        "售罄",
        "实名购票",
        "实名制",
        "退票",
        "退改",
        "限购",
        "购票链接",
        "购票渠道",
        "购票平台",
        "票星球",
        "大麦",
        "看个比赛",
        "猫眼",
    )

    TICKET_WEAK_WORDS = ("门票",)

    TICKET_TRANSACTION_WORDS = (
        "购买",
        "购票",
        "售票",
        "票价",
        "元",
        "¥",
        "￥",
        "开售",
        "预售",
        "限购",
        "实名",
        "开抢",
        "抢购",
    )

    BASKETBALL_WORDS = (
        "篮球",
        "辽篮",
        "篮坛",
        "篮球队",
        "cba",
        "wcba",
        "nbl",
        "男篮",
        "女篮",
        "国青",
        "u16",
        "u18",
        "u19",
        "u21",
        "篮协",
        "全明星",
        "俱乐部杯",
        "三人篮球",
        "超三",
    )

    PAGE_TICKET_WORDS = (
        "立即购票",
        "立即购买",
        "选择场次",
        "选择票档",
        "票价",
        "开售",
        "售票",
        "实名",
        "限购",
        "退票",
        "购买数量",
        "缺货登记",
        "暂无票",
        "售罄",
    )

    def __init__(
        self,
        feeds: List[RSSFeedConfig],
        request_interval: int = 2000,
        timeout: int = 15,
        use_proxy: bool = False,
        proxy_url: str = "",
        timezone: str = DEFAULT_TIMEZONE,
        freshness_enabled: bool = True,
        default_max_age_days: int = 3,
    ):
        self.feeds = [feed for feed in feeds if feed.enabled]
        self.request_interval = request_interval
        self.timeout = timeout
        self.use_proxy = use_proxy
        self.proxy_url = proxy_url
        self.timezone = timezone
        self.freshness_enabled = freshness_enabled
        self.default_max_age_days = default_max_age_days
        self.parser = RSSParser()
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """创建请求会话"""

        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "application/rss+xml, application/atom+xml, "
                    "application/xml;q=0.9, text/xml;q=0.9, "
                    "text/html;q=0.8, application/json;q=0.8, */*;q=0.7"
                ),
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

        retry = Retry(
            total=4,
            connect=4,
            read=4,
            status=4,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "HEAD"]),
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        if self.use_proxy and self.proxy_url:
            session.proxies = {
                "http": self.proxy_url,
                "https": self.proxy_url,
            }

        return session

    @staticmethod
    def _normalize_author(author: str) -> str:
        return re.sub(r"\s+", "", (author or "").strip()).lower()

    def _author_allowed(self, feed: RSSFeedConfig, author: str) -> bool:
        if not feed.allowed_authors:
            return True

        normalized = self._normalize_author(author)
        allowed = {
            self._normalize_author(item)
            for item in feed.allowed_authors
            if item
        }

        return normalized in allowed

    @staticmethod
    def _extract_links(fragment: str) -> List[str]:
        """从 RSS 原始 description / summary 中提取 HTTP 链接"""

        if not fragment:
            return []

        decoded = html.unescape(fragment)
        links: List[str] = []

        for match in re.findall(
            r'href\s*=\s*["\']([^"\']+)["\']',
            decoded,
            flags=re.IGNORECASE,
        ):
            links.append(match.strip())

        for match in re.findall(
            r'https?://[^\s<>"\']+',
            decoded,
            flags=re.IGNORECASE,
        ):
            links.append(match.rstrip(".,;，。；）)]}"))

        result: List[str] = []
        seen = set()

        for url in links:
            if not url.startswith(("http://", "https://")):
                continue
            if url in seen:
                continue
            seen.add(url)
            result.append(url)

        return result


    @staticmethod
    def _is_weibo_keyword_feed(feed: RSSFeedConfig) -> bool:
        """
        只对 RSSHub 微博 keyword discovery 做严格正文日期兜底。

        固定官方 UID（/weibo/user/<uid>）不走这套逻辑，
        避免因为 RSSHub 缺少 pubDate 而误杀刚发布的官方微博。
        """
        url = (feed.url or "").lower()
        return "/weibo/keyword/" in url

    @staticmethod
    def _extract_explicit_dates_from_text(
        text: str,
        now: datetime,
    ) -> List[datetime]:
        """
        从微博标题/正文中提取明确日期。

        支持：
        - 2026年6月2日
        - 6月2日
        - 2026-06-02 / 2026/06/02
        - 8.10 / 8.10号 / 8.10日

        无年份日期默认使用当前年份；
        如果解析结果比当前时间晚超过 180 天，则回退上一年，
        用于处理年初抓到上一年年末旧帖的情况。
        """
        if not text:
            return []

        normalized = re.sub(r"\s+", "", text)
        results: List[datetime] = []
        seen = set()

        def add_date(year: int, month: int, day: int):
            try:
                dt = now.replace(
                    year=year,
                    month=month,
                    day=day,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
            except ValueError:
                return

            # 无年份日期在跨年附近做简单回退。
            if dt - now > timedelta(days=180):
                try:
                    dt = dt.replace(year=year - 1)
                except ValueError:
                    pass

            key = dt.strftime("%Y-%m-%d")
            if key not in seen:
                seen.add(key)
                results.append(dt)

        # YYYY年M月D日
        for m in re.finditer(
            r"(?<!\d)(20\d{2})年(\d{1,2})月(\d{1,2})日?",
            normalized,
        ):
            add_date(
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
            )

        # YYYY-MM-DD / YYYY/MM/DD
        for m in re.finditer(
            r"(?<!\d)(20\d{2})[-/](\d{1,2})[-/](\d{1,2})(?!\d)",
            normalized,
        ):
            add_date(
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
            )

        # M月D日
        for m in re.finditer(
            r"(?<!\d)(\d{1,2})月(\d{1,2})日?",
            normalized,
        ):
            # 避免重复吃到 YYYY年M月D日中的 M月D日。
            prefix = normalized[max(0, m.start() - 6):m.start()]
            if re.search(r"20\d{2}年$", prefix):
                continue

            add_date(
                now.year,
                int(m.group(1)),
                int(m.group(2)),
            )

        # M.D / M.D号 / M.D日
        # 只接受 1-12 月和 1-31 日，且前后不能继续是数字。
        for m in re.finditer(
            r"(?<!\d)(1[0-2]|0?[1-9])\.(3[01]|[12]\d|0?[1-9])(?:号|日)?(?!\d)",
            normalized,
        ):
            add_date(
                now.year,
                int(m.group(1)),
                int(m.group(2)),
            )

        return sorted(results)

    def _should_drop_old_keyword_discovery_item(
        self,
        feed: RSSFeedConfig,
        title: str,
        summary: str,
        published_at: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        微博 keyword discovery 的日期兜底。

        规则：
        1. 非 /weibo/keyword/ 源：不处理。
        2. published_at 有值：交给 TrendRadar 原有 freshness 逻辑。
        3. published_at 为空：从标题+正文抽取明确日期。
        4. 如果存在至少一个最近/未来日期：保留。
        5. 如果所有明确日期都早于 max_age_days：丢弃。
        6. 完全没有明确日期：保留，但后续只能视为“时间未核实线索”。

        对票务 discovery 很重要：一条当前售票帖可能同时写旧赛果和未来场次；
        只要文本里存在最近/未来明确日期，就不会因为另一个旧日期被误删。
        """
        if not self._is_weibo_keyword_feed(feed):
            return False, None

        # keyword RSS 的 published_at 可能缺失，也可能并不可靠地代表微博原始发布时间。
        # 因此只要正文里存在明确日期，就优先用正文日期做“明显过旧”兜底；
        # 如果正文没有明确日期，再交给 TrendRadar 原有 published_at freshness 逻辑。

        if feed.max_age_days is None:
            return False, None

        try:
            max_age_days = int(feed.max_age_days)
        except (TypeError, ValueError):
            return False, None

        if max_age_days < 0:
            return False, None

        now = get_configured_time(self.timezone)
        dates = self._extract_explicit_dates_from_text(
            f"{title or ''} {summary or ''}",
            now,
        )

        if not dates:
            return False, None

        cutoff = (
            now.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            - timedelta(days=max_age_days)
        )

        # 只要有一个明确日期仍在窗口内或属于未来，就保留。
        if any(dt >= cutoff for dt in dates):
            return False, None

        newest = max(dates)
        return (
            True,
            newest.strftime("%Y-%m-%d"),
        )


    @staticmethod
    def _is_strict_ticket_discovery_feed(
        feed: RSSFeedConfig,
    ) -> bool:
        """
        仅对“票务专项｜...”源启用整条 RSS 严格过滤。

        这些源本质是 keyword discovery；若不是篮球票务，
        不应进入 RSS 数据库和后续关键词分组。
        """
        return (feed.name or "").startswith("票务专项｜")

    def _is_ticket_context(
        self,
        feed: RSSFeedConfig,
        title: str,
        summary: str,
    ) -> bool:
        """
        判断当前条目是否真的属于篮球票务。

        不使用 feed.name 参与篮球判断，避免
        “票务专项｜大麦篮球”让普通演唱会自动命中篮球。
        """

        _ = feed

        # 篮球属性只看标题/微博正文主体（RSSHub 的 title 通常就是整条微博正文）。
        # 不使用 summary 做篮球判定，因为部分 keyword RSS 的 summary 可能夹带
        # feed/query/关联文本，从而让演唱会、足球、网球等条目被“篮球”误命中。
        title_lower = (title or "").lower()
        ticket_text_lower = f"{title or ''} {summary or ''}".lower()

        has_basketball = any(
            word.lower() in title_lower
            for word in self.BASKETBALL_WORDS
        )

        if not has_basketball:
            return False

        has_strong_ticket = any(
            word.lower() in ticket_text_lower
            for word in self.TICKET_STRONG_WORDS
        )

        has_weak_ticket = any(
            word.lower() in ticket_text_lower
            for word in self.TICKET_WEAK_WORDS
        )

        has_transaction = any(
            word.lower() in ticket_text_lower
            for word in self.TICKET_TRANSACTION_WORDS
        )

        return has_strong_ticket or (has_weak_ticket and has_transaction)

    @staticmethod
    def _is_static_asset_url(url: str) -> bool:
        path = (urlparse(url).path or "").lower()

        static_extensions = (
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".webp",
            ".svg",
            ".mp4",
            ".mov",
            ".m3u8",
            ".css",
            ".js",
            ".woff",
            ".woff2",
            ".ttf",
            ".ico",
        )

        return path.endswith(static_extensions)

    @staticmethod
    def _is_weibo_or_sina_url(url: str) -> bool:
        host = (urlparse(url).hostname or "").lower()

        blocked_hosts = (
            "weibo.com",
            "weibo.cn",
            "sinaimg.cn",
            "weibocdn.com",
            "passport.weibo.cn",
            "visitor.passport.weibo.cn",
            "h5.sinaimg.cn",
            "n.sinaimg.cn",
            "video.weibo.com",
        )

        return any(
            host == blocked or host.endswith("." + blocked)
            for blocked in blocked_hosts
        )

    @staticmethod
    def _extract_visible_text(content: str, limit: int = 6000) -> str:
        parser = _VisibleTextParser()

        try:
            parser.feed(content)
            text = parser.get_text()
        except Exception:
            text = re.sub(r"<[^>]+>", " ", content)
            text = html.unescape(text)
            text = re.sub(r"\s+", " ", text).strip()

        return text[:limit]

    def _detect_ticket_platform(
        self,
        final_url: str,
        visible_text: str,
    ) -> Optional[str]:
        """
        严格识别真正票务平台。

        域名命中优先；
        页面文字命中时还要求存在实际票务交易词。
        """

        host = (urlparse(final_url).hostname or "").lower()
        text = (visible_text or "")[:6000].lower()

        if host == "damai.cn" or host.endswith(".damai.cn"):
            return "大麦"

        if "piaoxingqiu" in host:
            return "票星球"

        if "票星球" in text and any(
            word.lower() in text for word in self.PAGE_TICKET_WORDS
        ):
            return "票星球"

        if host == "maoyan.com" or host.endswith(".maoyan.com"):
            return "猫眼"

        if "猫眼演出" in text and any(
            word.lower() in text for word in self.PAGE_TICKET_WORDS
        ):
            return "猫眼"

        if "kangebisai" in host:
            return "看个比赛"

        if "看个比赛" in text and any(
            word.lower() in text for word in self.PAGE_TICKET_WORDS
        ):
            return "看个比赛"

        return None

    @staticmethod
    def _json_string(content: str, key: str) -> Optional[str]:
        pattern = (
            rf'"{re.escape(key)}"'
            r'\s*:\s*'
            r'"((?:\\.|[^"\\])*)"'
        )

        match = re.search(pattern, content)
        if not match:
            return None

        raw = match.group(1)

        try:
            return json.loads(f'"{raw}"')
        except Exception:
            return raw.replace(r"\/", "/")

    @staticmethod
    def _json_number(content: str, key: str) -> Optional[str]:
        match = re.search(
            rf'"{re.escape(key)}"\s*:\s*(-?\d+(?:\.\d+)?)',
            content,
        )

        if match:
            return match.group(1)

        return None

    @staticmethod
    def _json_bool(content: str, key: str) -> Optional[bool]:
        match = re.search(
            rf'"{re.escape(key)}"\s*:\s*(true|false)',
            content,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        return match.group(1).lower() == "true"

    def _extract_damai_evidence(
        self,
        content: str,
        visible_text: str,
    ) -> str:
        """从大麦页面提取结构化票务字段"""

        parts: List[str] = []

        field_map = (
            ("项目名", ("projectName", "itemName")),
            ("票价区间", ("priceRange",)),
            ("开售时间", ("sellStartTimeStr",)),
            ("销售状态", ("buyBtnText",)),
            ("比赛/演出日期", ("performStartDate",)),
            ("场次", ("performTime", "performTimeDetail")),
            ("城市", ("cityName",)),
            ("场馆", ("venueName",)),
        )

        for label, keys in field_map:
            value = None

            for key in keys:
                value = self._json_string(content, key)
                if value:
                    break

            if value:
                parts.append(f"{label}={value}")

        single_limit = self._json_number(content, "singleLimit")
        if single_limit:
            parts.append(f"单场限购={single_limit}张")

        real_name = self._json_bool(content, "needRealNameCertified")

        if real_name is not None:
            if real_name:
                parts.append("实名要求=需要实名/实人认证")
            else:
                parts.append("实名要求=页面结构字段显示无需实名认证")

        refund_label = None

        if "不支持退换" in visible_text or "不支持退" in visible_text:
            refund_label = "不支持退/退换"
        elif "有条件退款" in visible_text or "条件退" in visible_text:
            refund_label = "支持有条件退款"
        elif "支持退款" in visible_text:
            refund_label = "支持退款"

        if refund_label:
            parts.append(f"退改={refund_label}")

        if not single_limit:
            limit_match = re.search(
                r"(每笔订单最多购买\s*\d+\s*张|每个账号最多购买\s*\d+\s*张)",
                visible_text,
            )

            if limit_match:
                parts.append("限购=" + limit_match.group(1))

        return "；".join(parts)

    @staticmethod
    def _extract_piaoxingqiu_lss_id(url: str) -> str:
        """从票星球 H5 URL 中提取 lssId"""

        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        for key in ("lssId", "lssid", "lss_id"):
            value = query.get(key)
            if value and value[0]:
                return value[0]

        match = re.search(
            r"(?:lssId|lssid|lss_id)=([^&#]+)",
            url,
            flags=re.IGNORECASE,
        )

        if match:
            return match.group(1)

        return ""

    def _build_piaoxingqiu_restricted_evidence(
        self,
        final_url: str,
        status_code: int,
    ) -> str:
        """
        票星球项目已被发现，但详情页被风控阻止时，
        保留可验证的项目链接与项目 ID。
        """

        evidence_parts = [
            "平台=票星球",
            f"最终项目链接={final_url}",
            (
                f"访问状态=HTTP {status_code}，"
                "GitHub Actions 自动读取项目详情受限；"
                "该状态不等于项目不存在"
            ),
        ]

        lss_id = self._extract_piaoxingqiu_lss_id(final_url)

        if lss_id:
            evidence_parts.append(f"票星球项目ID={lss_id}")

        return "；".join(evidence_parts)

    def _fetch_ticket_page(self, source_url: str) -> Optional[str]:
        """
        跟随外链抓真实票务项目页面。

        特殊规则：
        - 微博/新浪内部页和静态资源直接排除。
        - 普通网页不能冒充票务页。
        - 票星球若返回 469，保留真实 URL + lssId + 访问受限状态。
        """

        if not source_url:
            return None

        if self._is_static_asset_url(source_url):
            return None

        if self._is_weibo_or_sina_url(source_url):
            return None

        source_host = (urlparse(source_url).hostname or "").lower()
        source_is_piaoxingqiu = "piaoxingqiu" in source_host

        try:
            response = self.session.get(
                source_url,
                timeout=(10, min(max(self.timeout, 20), 45)),
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            print(
                "[票务页] 外链请求异常: "
                f"{source_url} -> {exc}"
            )
            return None

        final_url = response.url or source_url
        final_host = (urlparse(final_url).hostname or "").lower()
        final_is_piaoxingqiu = "piaoxingqiu" in final_host

        if (
            response.status_code == 469
            and (source_is_piaoxingqiu or final_is_piaoxingqiu)
        ):
            print(
                "[票务页] 票星球项目已确认，"
                "但详情页返回 HTTP 469: "
                f"{final_url}"
            )

            return self._build_piaoxingqiu_restricted_evidence(
                final_url=final_url,
                status_code=response.status_code,
            )

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            print(
                "[票务页] 外链抓取失败: "
                f"{source_url} -> {exc}"
            )
            return None

        if self._is_static_asset_url(final_url):
            return None

        if self._is_weibo_or_sina_url(final_url):
            print(
                "[票务页] 忽略微博/新浪内部页: "
                f"{final_url}"
            )
            return None

        content_type = response.headers.get(
            "Content-Type",
            "",
        ).lower()

        allowed_types = (
            "text/html",
            "application/xhtml",
            "text/plain",
            "application/json",
        )

        if not any(
            marker in content_type
            for marker in allowed_types
        ):
            return None

        raw_content = response.text[:2_000_000]

        visible_text = self._extract_visible_text(
            raw_content,
            limit=6000,
        )

        platform = self._detect_ticket_platform(
            final_url,
            visible_text,
        )

        if not platform:
            print(
                "[票务页] 非票务平台页面，忽略: "
                f"{final_url}"
            )
            return None

        evidence_parts = [
            f"平台={platform}",
            f"最终项目链接={final_url}",
        ]

        if platform == "大麦":
            structured = self._extract_damai_evidence(
                raw_content,
                visible_text,
            )

            if structured:
                evidence_parts.append(structured)

        if platform == "票星球":
            lss_id = self._extract_piaoxingqiu_lss_id(final_url)
            if lss_id:
                evidence_parts.append(f"票星球项目ID={lss_id}")

        if visible_text:
            evidence_parts.append(
                "页面正文=" + visible_text[:3500]
            )

        print(
            "[票务页] 真正票务页面抓取成功: "
            f"{platform} -> {final_url}"
        )

        return "；".join(evidence_parts)

    def _enrich_ticket_summary(
        self,
        feed: RSSFeedConfig,
        title: str,
        summary: str,
        raw_summary: str = "",
    ) -> str:
        """对篮球票务条目进行项目页增强"""

        if not feed.ticket_enrich:
            return summary

        if not self._is_ticket_context(
            feed,
            title,
            summary,
        ):
            return summary

        print(
            f"[票务检测] {feed.name}: "
            f"命中篮球票务内容 -> {title[:80]}"
        )

        link_source = raw_summary or summary
        links = self._extract_links(link_source)

        if not links:
            print(
                f"[票务检测] {feed.name}: "
                "RSS 原始正文没有可跟随的 HTTP 链接"
            )
            return summary

        candidate_links: List[str] = []

        for link in links:
            if self._is_static_asset_url(link):
                continue

            if self._is_weibo_or_sina_url(link):
                continue

            candidate_links.append(link)

        print(
            f"[票务检测] {feed.name}: "
            f"{len(links)} 个原始链接，"
            f"{len(candidate_links)} 个可用外链候选"
        )

        if not candidate_links:
            print(
                f"[票务检测] {feed.name}: "
                "只有微博内部页/图片等链接，"
                "没有真实购票项目外链"
            )
            return summary

        evidence: List[str] = []

        for link in candidate_links[:6]:
            print(
                "[票务检测] "
                f"尝试票务外链 -> {link}"
            )

            item = self._fetch_ticket_page(link)

            if not item:
                continue

            evidence.append(item)

            if len(evidence) >= 2:
                break

        if not evidence:
            print(
                f"[票务检测] {feed.name}: "
                "发现候选外链，但没有获得真正票务平台项目页"
            )
            return summary

        appendix = "\n\n".join(
            (
                "[真实票务项目页证据 "
                f"{index}] {item}"
            )
            for index, item in enumerate(
                evidence,
                start=1,
            )
        )

        print(
            f"[票务检测] {feed.name}: "
            f"成功补充 {len(evidence)} 个票务项目证据"
        )

        return (
            f"{summary}\n\n"
            "========== 票务项目页补充证据 ==========\n"
            f"{appendix}"
        )

    def fetch_feed(
        self,
        feed: RSSFeedConfig,
    ) -> Tuple[List[RSSItem], Optional[str]]:
        """抓取单个 RSS 源"""

        try:
            request_url = feed.url

            response = self.session.get(
                request_url,
                timeout=(10, self.timeout),
                allow_redirects=True,
            )

            if (
                response.status_code == 403
                and request_url.startswith(
                    self.RSSHUB_FALLBACK_FROM
                )
            ):
                fallback_url = request_url.replace(
                    self.RSSHUB_FALLBACK_FROM,
                    self.RSSHUB_FALLBACK_TO,
                    1,
                )

                print(
                    f"[RSS] {feed.name}: "
                    "rsshub.app 返回403，"
                    "尝试备用实例: "
                    f"{fallback_url}"
                )

                response = self.session.get(
                    fallback_url,
                    timeout=(10, self.timeout),
                    allow_redirects=True,
                )

            response.raise_for_status()

            content_type = response.headers.get(
                "Content-Type",
                "",
            ).lower()

            content = response.text
            stripped = content.lstrip().lower()

            looks_like_feed = (
                stripped.startswith("<?xml")
                or stripped.startswith("<rss")
                or stripped.startswith("<feed")
                or stripped.startswith("{")
            )

            if not looks_like_feed:
                raise ValueError(
                    "返回内容不是 RSS/Atom/JSON Feed；"
                    f"status={response.status_code}, "
                    f"content_type={content_type}, "
                    f"final_url={response.url}, "
                    f"preview={content[:200]!r}"
                )

            parsed_items = self.parser.parse(
                content,
                feed.url,
            )

            if feed.max_items > 0:
                parsed_items = parsed_items[:feed.max_items]

            now = get_configured_time(self.timezone)
            crawl_time = now.strftime("%H:%M")

            items: List[RSSItem] = []
            author_filtered = 0

            for parsed in parsed_items:
                if not self._author_allowed(
                    feed,
                    parsed.author or "",
                ):
                    author_filtered += 1

                    print(
                        f"[RSS作者过滤] {feed.name}: "
                        f"拒绝 author={parsed.author!r}, "
                        f"title={(parsed.title or '')[:60]!r}"
                    )
                    continue

                clean_summary = parsed.summary or ""

                # ============================================
                # 微博 keyword discovery 正文日期兜底
                # ============================================

                should_drop_old, explicit_date = (
                    self._should_drop_old_keyword_discovery_item(
                        feed=feed,
                        title=parsed.title or "",
                        summary=clean_summary,
                        published_at=parsed.published_at or "",
                    )
                )

                if should_drop_old:
                    print(
                        f"[微博日期过滤] {feed.name}: "
                        f"正文最新明确日期={explicit_date}, "
                        f"超过 max_age_days={feed.max_age_days}, "
                        f"丢弃 -> {(parsed.title or '')[:80]}"
                    )
                    continue

                # ============================================
                # 票务专项 discovery：非篮球票务整条丢弃
                # ============================================

                if (
                    self._is_strict_ticket_discovery_feed(feed)
                    and not self._is_ticket_context(
                        feed,
                        parsed.title or "",
                        clean_summary,
                    )
                ):
                    print(
                        f"[票务专项过滤] {feed.name}: "
                        f"非篮球票务，整条丢弃 -> "
                        f"{(parsed.title or '')[:80]}"
                    )
                    continue

                raw_summary = (
                    getattr(
                        parsed,
                        "raw_summary",
                        None,
                    )
                    or clean_summary
                )

                enriched_summary = self._enrich_ticket_summary(
                    feed=feed,
                    title=parsed.title or "",
                    summary=clean_summary,
                    raw_summary=raw_summary,
                )

                item = RSSItem(
                    title=parsed.title,
                    feed_id=feed.id,
                    feed_name=feed.name,
                    url=parsed.url,
                    guid=parsed.guid or "",
                    published_at=parsed.published_at or "",
                    summary=enriched_summary,
                    author=parsed.author or "",
                    crawl_time=crawl_time,
                    first_time=crawl_time,
                    last_time=crawl_time,
                    count=1,
                )

                items.append(item)

            extra = ""

            if feed.allowed_authors:
                extra = (
                    ", 官方作者过滤后"
                    f"保留 {len(items)} 条"
                    f", 丢弃 {author_filtered} 条"
                )

            print(
                f"[RSS] {feed.name}: "
                f"获取 {len(items)} 条 "
                f"(status={response.status_code}, "
                f"content-type={content_type}"
                f"{extra})"
            )

            return items, None

        except requests.Timeout:
            error = f"请求超时 ({self.timeout}s)"
            print(f"[RSS] {feed.name}: {error}")
            return [], error

        except requests.RequestException as exc:
            error = f"请求失败: {exc}"
            print(f"[RSS] {feed.name}: {error}")
            return [], error

        except ValueError as exc:
            error = f"解析失败: {exc}"
            print(f"[RSS] {feed.name}: {error}")
            return [], error

        except Exception as exc:
            error = (
                "未知错误: "
                f"{type(exc).__name__}: {exc}"
            )

            print(f"[RSS] {feed.name}: {error}")
            return [], error

    def fetch_all(self) -> RSSData:
        """抓取所有 RSS 源"""

        all_items: Dict[str, List[RSSItem]] = {}
        id_to_name: Dict[str, str] = {}
        failed_ids: List[str] = []

        now = get_configured_time(self.timezone)
        crawl_time = now.strftime("%H:%M")
        crawl_date = now.strftime("%Y-%m-%d")

        print(
            "[RSS] 开始抓取 "
            f"{len(self.feeds)} 个 RSS 源..."
        )

        for index, feed in enumerate(self.feeds):
            if index > 0:
                interval = self.request_interval / 1000
                jitter = random.uniform(-0.2, 0.2) * interval
                time.sleep(max(0, interval + jitter))

            items, error = self.fetch_feed(feed)
            id_to_name[feed.id] = feed.name

            if error:
                failed_ids.append(feed.id)
            else:
                all_items[feed.id] = items

        total_items = sum(
            len(items)
            for items in all_items.values()
        )

        print(
            "[RSS] 抓取完成: "
            f"{len(all_items)} 个源成功, "
            f"{len(failed_ids)} 个失败, "
            f"共 {total_items} 条"
        )

        if failed_ids:
            print(
                "[RSS] 失败源 ID: "
                + ", ".join(failed_ids)
            )

        return RSSData(
            date=crawl_date,
            crawl_time=crawl_time,
            items=all_items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
        )

    @classmethod
    def from_config(
        cls,
        config: Dict,
    ) -> "RSSFetcher":
        """从配置字典创建抓取器"""

        freshness_config = config.get(
            "freshness_filter",
            {},
        )

        freshness_enabled = freshness_config.get(
            "enabled",
            True,
        )

        default_max_age_days = freshness_config.get(
            "max_age_days",
            3,
        )

        feeds: List[RSSFeedConfig] = []

        for feed_config in config.get("feeds", []):
            max_age_days_raw = feed_config.get(
                "max_age_days"
            )

            max_age_days = None

            if max_age_days_raw is not None:
                try:
                    max_age_days = int(
                        max_age_days_raw
                    )

                    if max_age_days < 0:
                        feed_id = feed_config.get(
                            "id",
                            "unknown",
                        )

                        print(
                            "[警告] "
                            f"RSS feed '{feed_id}' "
                            "的 max_age_days 为负数，"
                            "将使用全局默认值"
                        )

                        max_age_days = None

                except (ValueError, TypeError):
                    feed_id = feed_config.get(
                        "id",
                        "unknown",
                    )

                    print(
                        "[警告] "
                        f"RSS feed '{feed_id}' "
                        "的 max_age_days 格式错误："
                        f"{max_age_days_raw}"
                    )

                    max_age_days = None

            allowed_authors_raw = feed_config.get(
                "allowed_authors",
                [],
            )

            if isinstance(allowed_authors_raw, str):
                allowed_authors = (
                    [allowed_authors_raw.strip()]
                    if allowed_authors_raw.strip()
                    else []
                )

            elif isinstance(allowed_authors_raw, list):
                allowed_authors = [
                    str(author).strip()
                    for author in allowed_authors_raw
                    if str(author).strip()
                ]

            else:
                allowed_authors = []

            ticket_enrich_raw = feed_config.get(
                "ticket_enrich",
                False,
            )

            if isinstance(ticket_enrich_raw, bool):
                ticket_enrich = ticket_enrich_raw

            elif isinstance(ticket_enrich_raw, str):
                ticket_enrich = (
                    ticket_enrich_raw.strip().lower()
                    in {"1", "true", "yes", "on"}
                )

            else:
                ticket_enrich = bool(ticket_enrich_raw)

            feed = RSSFeedConfig(
                id=feed_config.get("id", ""),
                name=feed_config.get("name", ""),
                url=feed_config.get("url", ""),
                max_items=feed_config.get(
                    "max_items",
                    0,
                ),
                enabled=feed_config.get(
                    "enabled",
                    True,
                ),
                max_age_days=max_age_days,
                allowed_authors=allowed_authors,
                ticket_enrich=ticket_enrich,
            )

            if feed.id and feed.url:
                feeds.append(feed)

        return cls(
            feeds=feeds,
            request_interval=config.get(
                "request_interval",
                2000,
            ),
            timeout=config.get(
                "timeout",
                15,
            ),
            use_proxy=config.get(
                "use_proxy",
                False,
            ),
            proxy_url=config.get(
                "proxy_url",
                "",
            ),
            timezone=config.get(
                "timezone",
                DEFAULT_TIMEZONE,
            ),
            freshness_enabled=freshness_enabled,
            default_max_age_days=default_max_age_days,
        )
